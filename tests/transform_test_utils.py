# pyright: basic

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Protocol

from utils import CollectorActor

from nexus.v1 import (
    ContextId,
    ContextStore,
    Flow,
    NexusException,
    PipeToBus,
    SendEvent,
    Source,
    SubnetBuilder,
    SubnetRuntime,
    Transform,
)

DEFAULT_RUNTIME_SHUTDOWN_TIMEOUT_SECONDS = 1.5


def build_runtime[Input, Output](
    *,
    transform: Transform[Input, Output],
    context_store: ContextStore | None = None,
    pipe_to_bus: PipeToBus | None = None,
) -> tuple[
    SubnetRuntime,
    CollectorActor[Output],
    CollectorActor[NexusException],
    Source[Input],
]:
    builder = SubnetBuilder(nodes=[transform], context_store=context_store, pipe_to_bus=pipe_to_bus)
    processed_collector = CollectorActor[Output](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="transform-processed-collector",
    )
    error_collector = CollectorActor[NexusException](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="transform-error-collector",
    )
    upstream_source = Source[Input]("transform-upstream-source")
    runtime = (
        builder.add_flows(
            Flow.from_connectable(upstream_source).then(transform.sink),
            Flow.from_connectable(transform.ok).then(processed_collector.sink),
            Flow.from_connectable(transform.error).then(error_collector.sink),
        )
        .add_actors(processed_collector, error_collector)
        .build()
    )
    return runtime, processed_collector, error_collector, upstream_source


@dataclass(frozen=True)
class TransformActorTestSetup[Input, Output]:
    runtime: SubnetRuntime
    processed_collector: CollectorActor[Output]
    error_collector: CollectorActor[NexusException]
    upstream_source: Source[Input]

    @contextmanager
    def running(
        self,
        *,
        shutdown_timeout_seconds: float = DEFAULT_RUNTIME_SHUTDOWN_TIMEOUT_SECONDS,
    ) -> Iterator[None]:
        with self.runtime.running(shutdown_timeout_seconds=shutdown_timeout_seconds):
            yield

    def send(
        self,
        *,
        input_payload: Input,
        ctx_id: ContextId | None = None,
    ) -> ContextId:
        resolved_ctx_id = ctx_id
        if resolved_ctx_id is None:
            with self.runtime.context_store.create_context() as context:
                resolved_ctx_id = context.id
        self.runtime.pipe_to_bus.put(
            SendEvent(
                ctx_id=resolved_ctx_id,
                source=self.upstream_source,
                payload=input_payload,
            )
        )
        return resolved_ctx_id


class TransformActorTestSetupFactory(Protocol):
    def __call__[Input, Output](
        self,
        transform: Transform[Input, Output],
    ) -> TransformActorTestSetup[Input, Output]: ...
