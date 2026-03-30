from typing import override

from nexus.actors.task_result_store_provider import DEFAULT_TASK_RESULT_STORE_PROVIDER, TaskResultStoreProvider
from nexus.core.dsl.nodes import NodeId, NodeSinks, NodeSources, SinkName, Source, SourceName, Transform
from nexus.core.runtime.actor import Actor, ActorBuilder
from nexus.core.runtime.actor_patterns import TransformActor
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.events import PipeToBus
from nexus.core.runtime.nexus_task_types import NexusTaskName
from nexus.core.runtime.task_result_store import (
    SingleTaskResult,
    TaskResultStore,
    TaskResultToPersist,
)
from nexus.utils.exceptions import ExecutorFailureException, RetryTaskAfterExecutorFailureException


class TaskResultStorer[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
    Transform[
        TaskResultToPersist[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
        SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
    ],
    ActorBuilder,
):
    """Persists a prepared task result to the task result store and emits the stored result.
    If the result contains an executor failure, it is stored but also raises a retry signal.

    sink sink: TaskResultToPersist to store
    source task_result: persisted SingleTaskResult
    source error: storage failures or RetryTaskAfterExecutorFailureException
    """

    task_result: Source[SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]]
    task_name: NexusTaskName
    task_result_store_provider: TaskResultStoreProvider[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]

    def __init__(
        self,
        _id: NodeId,
        name: NexusTaskName,
        task_result_store_provider: TaskResultStoreProvider[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]
        | None = None,
    ) -> None:
        super().__init__(_id)
        self.task_name = name
        self.task_result_store_provider = task_result_store_provider or DEFAULT_TASK_RESULT_STORE_PROVIDER

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


class TaskResultStorerActor[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
    TransformActor[
        TaskResultToPersist[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
        SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
    ]
):
    store: TaskResultStore[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]
    storer_spec: TaskResultStorer[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]

    def __init__(
        self,
        spec: TaskResultStorer[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
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
        payload: TaskResultToPersist[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
    ) -> SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]:
        task_result = self.store.add_task_result(ctx, self.storer_spec.task_name, payload)
        if isinstance(payload.result.executor_output.output, ExecutorFailureException):
            # the executor produced an error result; we saved it, but we also want to retry the task
            raise RetryTaskAfterExecutorFailureException() from payload.result.executor_output.output
        return task_result
