from collections.abc import Callable
from typing import override

from pydantic import BaseModel

from nexus._internal.actors import ExecutorCommunicator, Routed
from nexus._internal.actors.executor_communicator import CommunicatorActor
from nexus._internal.core.runtime.actor import Actor, ActorBuilder
from nexus._internal.core.runtime.context_store import Context, ContextStore
from nexus._internal.core.runtime.events import MessagesToSend, PipeToBus, ReceiveEvent
from nexus._internal.utils.exceptions import EmbeddedExecutorFailureException


class EmbeddedExecutorCommunicator[InputModel: BaseModel, OutputModel: BaseModel](
    ExecutorCommunicator[InputModel, OutputModel], ActorBuilder
):
    """
    ExecutorCommunicator that runs the executor function in-process instead of calling a remote neuron.
    Takes a callable at construction time and invokes it directly for each input.

    sink input: routed request payload
    source processed: executor result or EmbeddedExecutorFailureException
    source error: internal/framework failures
    """

    type ExecutorFunc = Callable[[InputModel], OutputModel]

    executor_func: ExecutorFunc

    def __init__(
        self,
        _id: str,
        input_model: type[InputModel],
        output_model: type[OutputModel],
        executor_func: ExecutorFunc,
    ) -> None:
        super().__init__(_id, input_model=input_model, output_model=output_model)
        self.executor_func = executor_func

    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return EmbeddedExecutorCommunicatorActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class EmbeddedExecutorCommunicatorActor[InputModel: BaseModel, OutputModel: BaseModel](
    CommunicatorActor[InputModel, OutputModel]
):
    embedded_executor_spec: EmbeddedExecutorCommunicator[InputModel, OutputModel]

    def __init__(
        self,
        *,
        spec: EmbeddedExecutorCommunicator[InputModel, OutputModel],
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.embedded_executor_spec = spec

    @override
    def handle_input(self, ctx: Context, event: ReceiveEvent[Routed[InputModel]]) -> MessagesToSend:
        try:
            output = self.embedded_executor_spec.executor_func(event.payload.input)
            return self._processed_event(ctx.id, output)
        except Exception as exc:
            executor_failure = EmbeddedExecutorFailureException(
                "failure during execution of externally provided function"
            )
            executor_failure.__cause__ = exc
            return self._executor_error_event(ctx.id, executor_failure)
