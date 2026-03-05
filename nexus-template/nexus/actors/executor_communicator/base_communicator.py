from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, cast, override

from nexus.actors.neuron_router import Routed
from nexus.core.dsl.nodes import NodeSinks, NodeSources, Sink, SinkName, Source, SourceName, Transform
from nexus.core.runtime.actor import Actor, EventHandler
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.context_store_types import ContextId
from nexus.core.runtime.events import MessagesToSend, PipeToBus, ReceiveEvent, SendEvent
from nexus.utils.exceptions import ExecutorFailureException, NexusException


@dataclass(frozen=True)
class ProcessedInput[Input, Output]:
    """
    Executor processing result paired with the original input that produced it.

    `output` carries either:
    - validated executor output (`Output`), or
    - executor-side failure (`NexusException`, typically wrapped as `ExecutorFailureException`).
    """

    input: Input
    output: Output | NexusException


type InputValidator[Input] = Callable[[ContextId, Input], None]


class ExecutorCommunicator[Input, Output](Transform[Routed[Input], ProcessedInput[Routed[Input], Output]]):
    """
    Transport-agnostic contract for executor-facing communicators.

    An executor communicator bridges routed execution requests and executor results
    in the processing graph:
    - consumes request payloads on `input`
    - emits `ProcessedInput` on `processed` for both success and executor-side failure
    - emits internal/framework failures on `error`

    This class defines only the logical node interface and naming conventions.
    Concrete implementations provide transport/protocol details. The current codebase
    includes an async HTTP implementation (`AsyncHttpNeuronCommunicator`), but the
    same contract can be implemented for other transports such as sync HTTP,
    WebSocket, or RPC-based protocols.
    """

    input: Sink[Routed[Input]]
    processed: Source[ProcessedInput[Routed[Input], Output]]

    input_model: type[Input]
    output_model: type[Output]

    def __init__(
        self,
        _id: str,
        input_model: type[Input],
        output_model: type[Output],
    ) -> None:
        super().__init__(_id)

        self.input_model = input_model
        self.output_model = output_model

        # alias for convenience
        self.input = self.sink
        self.processed = self.ok

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks(sinks={SinkName("input"): self.input})

    @override
    def sources(self) -> NodeSources:
        return NodeSources(
            sources={
                SourceName("processed"): self.processed,
                SourceName("error"): self.error,
            },
            default_source=self.processed,
        )


class CommunicatorActor[Input, Output](Actor, ABC):
    """
    Shared runtime actor base for executor communicator implementations.

    Provides common context/user-data behavior and event helpers:
    - load original input from context
    - build `SendEvent` values for successful executor outputs
    - build `SendEvent` values for wrapped executor-side failures
    - build `SendEvent` values for framework/internal failures
    - emit any prebuilt event through `_emit`

    The communicator node spec is stored privately and exposed through `_spec`
    as a read-only accessor for subclasses.
    """

    __spec: ExecutorCommunicator[Input, Output]
    _input_user_data_key: str

    def __init__(
        self,
        *,
        spec: ExecutorCommunicator[Input, Output],
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(name=spec.id, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.__spec = spec
        self._input_user_data_key = f"{self.__spec.id}-saved-input"

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {
            self.__spec.input: self._handle_input,
        }

    def _handle_input(self, ctx: Context, event: ReceiveEvent[Routed[Input]]) -> MessagesToSend:
        self._store_input_in_context(ctx, event.payload)
        return self.handle_input(ctx, event)

    @abstractmethod
    def handle_input(self, ctx: Context, event: ReceiveEvent[Routed[Input]]) -> MessagesToSend:
        pass

    def _store_input_in_context(self, ctx: Context, payload: Routed[Input]) -> None:
        ctx.set_user_data(self._input_user_data_key, payload)

    def _load_input_from_context(self, ctx_id: ContextId) -> Routed[Input]:
        # `expected_type=Routed` validates the outer container at runtime.
        # We load as `Routed[Any]`, then validate and narrow the inner payload.
        communicator_input = cast(
            Routed[Any],
            self.context_store.get_user_data(
                ctx_id,
                self._input_user_data_key,
                expected_type=Routed,
            ),
        )

        return cast(Routed[Input], communicator_input)

    def _processed_event(self, ctx_id: ContextId, payload: Output) -> SendEvent[ProcessedInput[Routed[Input], Output]]:
        communicator_input = self._load_input_from_context(ctx_id)
        processed = ProcessedInput(
            input=communicator_input,
            output=payload,
        )
        return SendEvent(
            ctx_id=ctx_id,
            source=self.__spec.processed,
            payload=processed,
        )

    def _executor_error_event(
        self, ctx_id: ContextId, error: NexusException
    ) -> SendEvent[ProcessedInput[Routed[Input], Output]]:
        communicator_input = self._load_input_from_context(ctx_id)
        processed: ProcessedInput[Routed[Input], Output] = ProcessedInput(
            input=communicator_input,
            output=ExecutorFailureException(error),
        )
        return SendEvent(
            ctx_id=ctx_id,
            source=self.__spec.processed,
            payload=processed,
        )

    def _internal_error_event(self, ctx_id: ContextId, error: NexusException) -> SendEvent[NexusException]:
        return SendEvent(
            ctx_id=ctx_id,
            source=self.__spec.error,
            payload=error,
        )

    def _emit_processed(self, ctx_id: ContextId, payload: Output) -> None:
        """
        Temporary compatibility wrapper for handlers that still emit side-effectfully.

        New code should prefer `_processed_event` and explicit `_emit`.
        """

        self._emit(self._processed_event(ctx_id, payload))

    def _emit_executor_error(self, ctx_id: ContextId, error: NexusException) -> None:
        """
        Temporary compatibility wrapper for handlers that still emit side-effectfully.

        New code should prefer `_executor_error_event` and explicit `_emit`.
        """

        self._emit(self._executor_error_event(ctx_id, error))

    def _emit_internal_error(self, ctx_id: ContextId, error: NexusException) -> None:
        """
        Temporary compatibility wrapper for handlers that still emit side-effectfully.

        New code should prefer `_internal_error_event` and explicit `_emit`.
        """

        self._emit(self._internal_error_event(ctx_id, error))

    def _emit(self, event: SendEvent[Any]) -> None:
        self._pipe_to_bus.put(event)
