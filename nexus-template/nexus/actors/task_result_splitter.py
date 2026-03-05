from typing import Any, override

from nexus.core.dsl.nodes import Node, NodeSinks, NodeSources, Sink, SinkName, Source, SourceName
from nexus.core.runtime.actor import Actor, ActorBuilder, EventHandler
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.events import MessagesToSend, PipeToBus, ReceiveEvent, SendEvent
from nexus.core.runtime.task_result_store import SingleTaskResult
from nexus.utils.exceptions import NexusException


class TaskResultSplitter[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](Node, ActorBuilder):
    """Splits one stored task result into two branches with different context semantics."""

    task_result_input: Sink[SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]]
    task_result: Source[SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]]
    executor_output: Source[ExecutorPublicOutput | NexusException]

    def __init__(self, _id: str) -> None:
        super().__init__(_id)
        self.task_result_input = Sink[SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]](
            f"{self.id}-task-result-input"
        )
        self.task_result = Source[SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]](
            f"{self.id}-task-result"
        )
        self.executor_output = Source[ExecutorPublicOutput | NexusException](f"{self.id}-executor-output")

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks(sinks={SinkName("task-result-input"): self.task_result_input})

    @override
    def sources(self) -> NodeSources:
        return NodeSources(
            sources={
                SourceName("task-result"): self.task_result,
                SourceName("executor-output"): self.executor_output,
            }
        )

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return TaskResultSplitterActor(
            spec=self,
            pipe_to_bus=pipe_to_bus,
            context_store=context_store,
        )


class TaskResultSplitterActor[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](Actor):
    """Emits executor output on the parent context and task result on a fresh child context."""

    spec: TaskResultSplitter[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]

    def __init__(
        self,
        *,
        spec: TaskResultSplitter[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(name=spec.id, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.spec = spec

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {self.spec.task_result_input: self.handle_task_result}

    def handle_task_result(
        self,
        ctx: Context,
        event: ReceiveEvent[SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]],
    ) -> MessagesToSend:
        with self.context_store.create_context(parents=(ctx.id,)) as child_context:
            child_context_id = child_context.id

        if event.payload.executor_public_output is None:
            executor_output_payload = event.payload.executor_output
        else:
            executor_output_payload = event.payload.executor_public_output

        return (
            SendEvent(
                ctx_id=child_context_id,
                source=self.spec.task_result,
                payload=event.payload,
            ),
            SendEvent(
                ctx_id=ctx.id,
                source=self.spec.executor_output,
                payload=executor_output_payload,
            ),
        )
