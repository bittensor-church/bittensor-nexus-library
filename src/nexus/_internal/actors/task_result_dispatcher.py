from typing import Any, override

from nexus._internal.core.dsl.nodes import Node, NodeSinks, NodeSources, Sink, SinkName, Source, SourceName
from nexus._internal.core.runtime.actor import Actor, ActorBuilder, EventHandler
from nexus._internal.core.runtime.context_store import Context, ContextStore
from nexus._internal.core.runtime.events import MessagesToSend, PipeToBus, ReceiveEvent, SendEvent
from nexus._internal.core.runtime.task_result_store import ExecutorFailureTaskResult, SuccessfulTaskResult


class TaskResultDispatcher[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](Node, ActorBuilder):
    """Dispatch typed stored task-result branches with the required context semantics."""

    successful_task_result_input: Sink[SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]]
    executor_failure_input: Sink[ExecutorFailureTaskResult[ExecutorPayload]]
    successful_task_result: Source[SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]]
    executor_failure: Source[ExecutorFailureTaskResult[ExecutorPayload]]
    executor_output: Source[ExecutorPublicOutput]

    def __init__(self, _id: str) -> None:
        super().__init__(_id)
        self.successful_task_result_input = Sink[
            SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]
        ](
            f"{self.id}-successful-task-result-input",
            owner_node=self,
        )
        self.executor_failure_input = Sink[ExecutorFailureTaskResult[ExecutorPayload]](
            f"{self.id}-executor-failure-input",
            owner_node=self,
        )
        self.successful_task_result = Source[
            SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]
        ](
            f"{self.id}-successful-task-result",
            owner_node=self,
        )
        self.executor_failure = Source[ExecutorFailureTaskResult[ExecutorPayload]](
            f"{self.id}-executor-failure",
            owner_node=self,
        )
        self.executor_output = Source[ExecutorPublicOutput](
            f"{self.id}-executor-output",
            owner_node=self,
        )

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks(
            sinks={
                SinkName("successful-task-result-input"): self.successful_task_result_input,
                SinkName("executor-failure-input"): self.executor_failure_input,
            }
        )

    @override
    def sources(self) -> NodeSources:
        return NodeSources(
            sources={
                SourceName("successful-task-result"): self.successful_task_result,
                SourceName("executor-failure"): self.executor_failure,
                SourceName("executor-output"): self.executor_output,
            }
        )

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return TaskResultDispatcherActor(
            spec=self,
            pipe_to_bus=pipe_to_bus,
            context_store=context_store,
        )


class TaskResultDispatcherActor[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](Actor):
    """Emit typed task-result branches on child contexts and success output on the parent context."""

    spec: TaskResultDispatcher[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]

    def __init__(
        self,
        *,
        spec: TaskResultDispatcher[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(name=spec.id, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.spec = spec

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {
            self.spec.successful_task_result_input: self.handle_successful_task_result,
            self.spec.executor_failure_input: self.handle_executor_failure,
        }

    def handle_successful_task_result(
        self,
        ctx: Context,
        event: ReceiveEvent[SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]],
    ) -> MessagesToSend:
        with self.context_store.create_context(parents=(ctx.id,)) as child_context:
            child_context_id = child_context.id

        return (
            SendEvent(
                ctx_id=child_context_id,
                source=self.spec.successful_task_result,
                payload=event.payload,
            ),
            SendEvent(
                ctx_id=ctx.id,
                source=self.spec.executor_output,
                payload=event.payload.executor_public_output,
            ),
        )

    def handle_executor_failure(
        self,
        ctx: Context,
        event: ReceiveEvent[ExecutorFailureTaskResult[ExecutorPayload]],
    ) -> MessagesToSend:
        with self.context_store.create_context(parents=(ctx.id,)) as child_context:
            child_context_id = child_context.id

        return (
            SendEvent(
                ctx_id=child_context_id,
                source=self.spec.executor_failure,
                payload=event.payload,
            ),
        )
