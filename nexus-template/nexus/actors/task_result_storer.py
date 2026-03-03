from typing import override

from nexus.actors import Timestamped
from nexus.actors.executor_communicator import ProcessedInput
from nexus.actors.task_result_store_provider import TaskResultStoreProvider
from nexus.core.dsl.nodes import NodeId, NodeSinks, NodeSources, SinkName, Source, SourceName, Transform
from nexus.core.runtime.actor import Actor, ActorBuilder
from nexus.core.runtime.actor_patterns import TransformActor
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.events import PipeToBus
from nexus.core.runtime.nexus_task_types import NexusTaskName
from nexus.core.runtime.task_result_store import TaskResultId, TaskResultStore
from nexus.utils.exceptions import ExecutorFailureException, RetryTaskAfterExecutorFailureException


class TaskResultStorer[Input, Output](
    Transform[Timestamped[ProcessedInput[Input, Output]], TaskResultId],
    ActorBuilder,
):
    task_result_ids: Source[TaskResultId]
    task_name: NexusTaskName
    task_result_store_provider: TaskResultStoreProvider[ProcessedInput[Input, Output]]

    def __init__(
        self,
        _id: NodeId,
        name: NexusTaskName,
        task_result_store_provider: TaskResultStoreProvider[ProcessedInput[Input, Output]],
    ) -> None:
        super().__init__(_id)
        self.task_name = name
        self.task_result_store_provider = task_result_store_provider

        # aliases for convenience
        self.task_result_ids = self.ok

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks(sinks={SinkName("result-input"): self.sink})

    @override
    def sources(self) -> NodeSources:
        return NodeSources(
            sources={
                SourceName("task-result-ids"): self.task_result_ids,
                SourceName("error"): self.error,
            },
            default_source=self.task_result_ids,
        )

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return TaskResultStorerActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class TaskResultStorerActor[Input, Output](
    TransformActor[Timestamped[ProcessedInput[Input, Output]], TaskResultId]
):
    store: TaskResultStore[ProcessedInput[Input, Output]]
    storer_spec: TaskResultStorer[Input, Output]

    def __init__(
        self,
        spec: TaskResultStorer[Input, Output],
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.storer_spec = spec
        self.store = spec.task_result_store_provider.get_task_result_store()

    @override
    def _transform(self, ctx: Context, payload: Timestamped[ProcessedInput[Input, Output]]) -> TaskResultId:
        task_result_id = self.store.add_task_result(ctx, self.storer_spec.task_name, payload)
        if isinstance(payload.executor_output.output, ExecutorFailureException):
            # the executor produced an error result; we saved it, but we also want to retry the task
            raise RetryTaskAfterExecutorFailureException() from payload.executor_output.output
        return task_result_id
