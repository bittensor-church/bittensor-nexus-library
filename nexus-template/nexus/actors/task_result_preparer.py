from typing import Any, cast, override

from nexus.actors.timestamper import Timestamped
from nexus.core.dsl.nodes import Node, NodeSinks, NodeSources, Sink, SinkName, Source, SourceName
from nexus.core.runtime.actor import Actor, ActorBuilder, EventHandler
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.events import MessagesToSend, PipeToBus, ReceiveEvent, SendEvent
from nexus.core.runtime.task_result_store import StoredTaskExecution, TaskResultToPersist
from nexus.utils.exceptions import InternalFrameworkException, NexusException


class TaskResultPreparer[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](Node, ActorBuilder):
    """Assembles a persistable task result from a timestamped executor result and its public output.
    The public output is produced asynchronously: the raw executor output is sent out for external
    conversion via `executor_output_for_conversion`. Connect a conversion pipeline from that source
    back to `converted_public_output` (or `conversion_failed` on error).
    Failed results skip conversion and are emitted directly.

    sink timestamped_result: timestamped executor result from the pipeline
    sink converted_public_output: public output returned from the conversion pipeline
    sink conversion_failed: failure from the conversion pipeline, clears pending state
    source executor_output_for_conversion: raw output to send into the conversion pipeline
    source prepared_task_result: final TaskResultToPersist ready for storage
    source error: internal failures (e.g. duplicate or missing results)
    """

    timestamped_result: Sink[StoredTaskExecution[ExecutorPayload, ExecutorOutput]]
    converted_public_output: Sink[ExecutorPublicOutput]
    conversion_failed: Sink[NexusException]

    executor_output_for_conversion: Source[ExecutorOutput]
    prepared_task_result: Source[TaskResultToPersist[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]]
    error: Source[NexusException]

    def __init__(self, _id: str) -> None:
        super().__init__(_id)
        self.timestamped_result = Sink[StoredTaskExecution[ExecutorPayload, ExecutorOutput]](
            f"{self.id}-timestamped-result",
            owner_node=self,
        )
        self.converted_public_output = Sink[ExecutorPublicOutput](f"{self.id}-converted-public-output", owner_node=self)
        self.conversion_failed = Sink[NexusException](f"{self.id}-conversion-failed", owner_node=self)
        self.executor_output_for_conversion = Source[ExecutorOutput](
            f"{self.id}-executor-output-for-conversion",
            owner_node=self,
        )
        self.prepared_task_result = Source[TaskResultToPersist[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]](
            f"{self.id}-prepared-task-result",
            owner_node=self,
        )
        self.error = Source[NexusException](f"{self.id}-error", owner_node=self)

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks(
            sinks={
                SinkName("timestamped-result"): self.timestamped_result,
                SinkName("converted-public-output"): self.converted_public_output,
                SinkName("conversion-failed"): self.conversion_failed,
            }
        )

    @override
    def sources(self) -> NodeSources:
        return NodeSources(
            sources={
                SourceName("executor-output-for-conversion"): self.executor_output_for_conversion,
                SourceName("prepared-task-result"): self.prepared_task_result,
                SourceName("error"): self.error,
            }
        )

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return TaskResultPreparerActor(
            spec=self,
            pipe_to_bus=pipe_to_bus,
            context_store=context_store,
        )


class TaskResultPreparerActor[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](Actor):
    spec: TaskResultPreparer[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]
    _pending_result_user_data_key: str

    def __init__(
        self,
        *,
        spec: TaskResultPreparer[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(name=spec.id, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.spec = spec
        self._pending_result_user_data_key = f"{self.spec.id}-pending-successful-result"

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {
            self.spec.timestamped_result: self._handle_timestamped_result,
            self.spec.converted_public_output: self._handle_converted_public_output,
            self.spec.conversion_failed: self._handle_conversion_failed,
        }

    def _handle_timestamped_result(
        self,
        ctx: Context,
        event: ReceiveEvent[StoredTaskExecution[ExecutorPayload, ExecutorOutput]],
    ) -> MessagesToSend:
        output = event.payload.executor_output.output
        if isinstance(output, NexusException):
            prepared_result = TaskResultToPersist[
                ExecutorPayload,
                ExecutorOutput,
                ExecutorPublicOutput,
            ](
                result=event.payload,
                executor_public_output=None,
            )
            return (
                SendEvent(
                    ctx_id=ctx.id,
                    source=self.spec.prepared_task_result,
                    payload=prepared_result,
                ),
            )

        if (
            self._pending_result_user_data_key in ctx.user_data
            and ctx.user_data[self._pending_result_user_data_key] is not None
        ):
            return (
                SendEvent(
                    ctx_id=ctx.id,
                    source=self.spec.error,
                    payload=InternalFrameworkException(
                        f"Received duplicate successful timestamped result for context {ctx.id}."
                    ),
                ),
            )

        ctx.set_user_data(self._pending_result_user_data_key, event.payload)
        return (
            SendEvent(
                ctx_id=ctx.id,
                source=self.spec.executor_output_for_conversion,
                payload=output,
            ),
        )

    def _handle_converted_public_output(
        self,
        ctx: Context,
        event: ReceiveEvent[ExecutorPublicOutput],
    ) -> MessagesToSend:
        if self._pending_result_user_data_key not in ctx.user_data:
            return (
                SendEvent(
                    ctx_id=ctx.id,
                    source=self.spec.error,
                    payload=InternalFrameworkException(
                        f"Missing pending successful timestamped result for context {ctx.id}."
                    ),
                ),
            )
        pending_result = ctx.user_data[self._pending_result_user_data_key]
        if not isinstance(pending_result, Timestamped):
            return (
                SendEvent(
                    ctx_id=ctx.id,
                    source=self.spec.error,
                    payload=InternalFrameworkException(
                        f"Unexpected pending successful timestamped result type for context {ctx.id}: "
                        f"{type(pending_result)!r}."
                    ),
                ),
            )
        typed_pending_result = cast(StoredTaskExecution[ExecutorPayload, ExecutorOutput], pending_result)
        ctx.set_user_data(self._pending_result_user_data_key, None)

        return (
            SendEvent(
                ctx_id=ctx.id,
                source=self.spec.prepared_task_result,
                payload=TaskResultToPersist(
                    result=typed_pending_result,
                    executor_public_output=event.payload,
                ),
            ),
        )

    def _handle_conversion_failed(
        self,
        ctx: Context,
        _: ReceiveEvent[NexusException],
    ) -> MessagesToSend:
        if self._pending_result_user_data_key in ctx.user_data:
            ctx.set_user_data(self._pending_result_user_data_key, None)
        return ()
