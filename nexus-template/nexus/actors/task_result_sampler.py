from typing import override

from nexus.core.dsl.nodes import NodeSinks, NodeSources, Sink, SinkName, Source, SourceName, Transform
from nexus.core.runtime.actor import Actor, ActorBuilder
from nexus.core.runtime.actor_patterns import TransformActor
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.events import PipeToBus
from nexus.core.runtime.task_result_store import SingleTaskResult


class TaskResultSampler[ExecutorPayload, Output](
    Transform[SingleTaskResult[ExecutorPayload, Output], tuple[SingleTaskResult[ExecutorPayload, Output], ...]]
):
    task_results: Sink[SingleTaskResult[ExecutorPayload, Output]]
    sampled_batch: Source[tuple[SingleTaskResult[ExecutorPayload, Output], ...]]

    def __init__(
        self,
        _id: str,
    ) -> None:
        super().__init__(_id)

        # alias for convenience
        self.task_results = self.sink
        self.sampled_batch = self.ok

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks(sinks={SinkName("task-results"): self.task_results})

    @override
    def sources(self) -> NodeSources:
        return NodeSources(
            sources={
                SourceName("sampled-batch"): self.sampled_batch,
                SourceName("error"): self.error,
            },
            default_source=self.sampled_batch,
        )


class EveryTaskResultSampler[ExecutorPayload, Output](TaskResultSampler[ExecutorPayload, Output], ActorBuilder):
    def __init__(
        self,
        _id: str,
    ) -> None:
        super().__init__(_id)

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return EveryTaskResultSamplerActor[ExecutorPayload, Output](
            spec=self,
            pipe_to_bus=pipe_to_bus,
            context_store=context_store,
        )


class EveryTaskResultSamplerActor[ExecutorPayload, Output](
    TransformActor[
        SingleTaskResult[ExecutorPayload, Output],
        tuple[SingleTaskResult[ExecutorPayload, Output], ...],
    ]
):
    def __init__(
        self,
        *,
        spec: EveryTaskResultSampler[ExecutorPayload, Output],
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.embedded_executor_spec = spec

    def _transform(
        self,
        ctx: Context,
        payload: SingleTaskResult[ExecutorPayload, Output],
    ) -> tuple[SingleTaskResult[ExecutorPayload, Output], ...]:
        return (payload,)
