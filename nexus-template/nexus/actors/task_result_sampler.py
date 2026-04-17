from typing import override

from nexus.core.dsl.nodes import NodeSinks, NodeSources, Sink, SinkName, Source, SourceName, Transform
from nexus.core.runtime.actor import Actor, ActorBuilder
from nexus.core.runtime.actor_patterns import TransformActor
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.events import PipeToBus
from nexus.core.runtime.task_result_store import SuccessfulTaskResult
class TaskResultSampler[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
    Transform[
        SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
        tuple[SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput], ...],
    ],
    ActorBuilder,
):
    """Base transform that batches individual task results for downstream processing (e.g. validation).
    Subclasses define the sampling strategy and when batches are emitted.

    sink task_results: individual task results
    source sampled_batch: batch of sampled task results
    source error: sampling failures
    """

    task_results: Sink[SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]]
    sampled_batch: Source[tuple[SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput], ...]]

    def __init__(self, _id: str) -> None:
        super().__init__(_id)
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


class EveryTaskResultSampler[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
    TaskResultSampler[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput], ActorBuilder
):
    """TaskResultSampler that emits immediately for every result as a single-element batch.

    sink task_results: individual task results
    source sampled_batch: single-element batch containing the input result
    source error: sampling failures
    """

    def __init__(
        self,
        _id: str,
    ) -> None:
        super().__init__(_id)

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return EveryTaskResultSamplerActor[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
            spec=self,
            pipe_to_bus=pipe_to_bus,
            context_store=context_store,
        )


class EveryTaskResultSamplerActor[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
    TransformActor[
        SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
        tuple[SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput], ...],
    ]
):
    """Runtime actor that validates and forwards successful task results as singleton batches."""

    def __init__(
        self,
        *,
        spec: EveryTaskResultSampler[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)

    @override
    def _transform(
        self,
        ctx: Context,
        payload: SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
    ) -> tuple[SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput], ...]:
        return (payload,)


__all__ = ["EveryTaskResultSampler", "EveryTaskResultSamplerActor"]
