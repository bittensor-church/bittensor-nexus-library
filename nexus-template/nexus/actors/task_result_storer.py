from typing import override

from nexus.actors.task_result_store_provider import TaskResultStoreProvider
from nexus.core.dsl.nodes import NodeId, NodeSinks, NodeSources, SinkName, Source, SourceName, Transform
from nexus.core.runtime.actor import Actor, ActorBuilder
from nexus.core.runtime.actor_patterns import TransformActor
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.events import PipeToBus
from nexus.core.runtime.nexus_task_types import NexusTaskName
from nexus.core.runtime.task_result_store import SingleTaskResult, StoredTaskExecution, TaskResultStore
from nexus.utils.exceptions import ExecutorFailureException, RetryTaskAfterExecutorFailureException


class TaskResultStorer[ExecutorPayload, Output](
    Transform[StoredTaskExecution[ExecutorPayload, Output], SingleTaskResult[ExecutorPayload, Output]],
    ActorBuilder,
):
    task_result: Source[SingleTaskResult[ExecutorPayload, Output]]
    task_name: NexusTaskName
    task_result_store_provider: TaskResultStoreProvider[ExecutorPayload, Output]

    def __init__(
        self,
        _id: NodeId,
        name: NexusTaskName,
        task_result_store_provider: TaskResultStoreProvider[ExecutorPayload, Output],
    ) -> None:
        super().__init__(_id)
        self.task_name = name
        self.task_result_store_provider = task_result_store_provider

        # aliases for convenience
        self.task_result = self.ok

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks(sinks={SinkName("result-input"): self.sink})

    @override
    def sources(self) -> NodeSources:
        return NodeSources(
            sources={
                SourceName("task_result"): self.task_result,
                SourceName("error"): self.error,
            },
            default_source=self.task_result,
        )

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return TaskResultStorerActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class TaskResultStorerActor[ExecutorPayload, Output](
    TransformActor[StoredTaskExecution[ExecutorPayload, Output], SingleTaskResult[ExecutorPayload, Output]]
):
    store: TaskResultStore[ExecutorPayload, Output]
    storer_spec: TaskResultStorer[ExecutorPayload, Output]

    def __init__(
        self,
        spec: TaskResultStorer[ExecutorPayload, Output],
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.storer_spec = spec
        self.store = spec.task_result_store_provider.get_task_result_store()

    @override
    def _transform(
        self,
        ctx: Context,
        payload: StoredTaskExecution[ExecutorPayload, Output],
    ) -> SingleTaskResult[ExecutorPayload, Output]:
        task_result = self.store.add_task_result(ctx, self.storer_spec.task_name, payload)
        if isinstance(payload.executor_output.output, ExecutorFailureException):
            # the executor produced an error result; we saved it, but we also want to retry the task
            raise RetryTaskAfterExecutorFailureException() from payload.executor_output.output
        return task_result
