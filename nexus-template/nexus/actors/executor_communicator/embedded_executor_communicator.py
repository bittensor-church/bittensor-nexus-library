from typing import Callable, override

from nexus.utils.exceptions import EmbeddedExecutorFailureException
from pydantic import BaseModel

from nexus.actors import ExecutorCommunicator, Routed
from nexus.actors.executor_communicator import CommunicatorActor
from nexus.core.runtime.actor import ActorBuilder, Actor
from nexus.core.runtime.context_store import ContextStore, Context
from nexus.core.runtime.events import PipeToBus, MessagesToSend, ReceiveEvent


class EmbeddedExecutorCommunicator[InputModel: BaseModel, OutputModel: BaseModel](
    ExecutorCommunicator[InputModel, OutputModel], ActorBuilder
):
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
            executor_failure = EmbeddedExecutorFailureException("failure during execution of externally provided function")
            executor_failure.__cause__ = exc
            return self._executor_error_event(ctx.id, executor_failure)

