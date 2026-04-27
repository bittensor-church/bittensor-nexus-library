from typing import Any, override

from nexus._internal.actors.task_result_store_provider import (
    DEFAULT_TASK_RESULT_STORE_PROVIDER,
    TaskResultStoreProvider,
)
from nexus._internal.core.dsl.nodes import NodeId, NodeSinks, NodeSources, Sink, SinkName, Source, SourceName, Transform
from nexus._internal.core.runtime.actor import Actor, ActorBuilder, EventHandler
from nexus._internal.core.runtime.actor_patterns import TransformActor
from nexus._internal.core.runtime.context_store import Context, ContextStore
from nexus._internal.core.runtime.events import MessagesToSend, PipeToBus, ReceiveEvent, SendEvent
from nexus._internal.core.runtime.nexus_task_types import NexusTaskName
from nexus._internal.core.runtime.task_result_store import (
    ExecutorFailureTaskResult,
    ExecutorFailureTaskResultToPersist,
    SuccessfulTaskResult,
    SuccessfulTaskResultToPersist,
    TaskResultStore,
)
from nexus._internal.utils.exceptions import InternalFrameworkException, RetryTaskAfterExecutorFailureException


class SuccessfulTaskResultStorer[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
    Transform[
        SuccessfulTaskResultToPersist[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
        SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
    ],
    ActorBuilder,
):
    """
    Persist one successful task result and emit the stored record.

    sink sink: SuccessfulTaskResultToPersist to store
    source successful_task_result: persisted SuccessfulTaskResult
    source error: storage failures
    """

    successful_task_result: Source[SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]]
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
        self.successful_task_result = self.ok

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks(sinks={SinkName("result-input"): self.sink})

    @override
    def sources(self) -> NodeSources:
        return NodeSources(
            sources={
                SourceName("successful_task_result"): self.successful_task_result,
                SourceName("error"): self.error,
            },
            default_source=self.successful_task_result,
        )

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return SuccessfulTaskResultStorerActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class SuccessfulTaskResultStorerActor[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
    TransformActor[
        SuccessfulTaskResultToPersist[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
        SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
    ]
):
    """Persist one successful task result and emit the stored record."""

    store: TaskResultStore[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]
    storer_spec: SuccessfulTaskResultStorer[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]

    def __init__(
        self,
        spec: SuccessfulTaskResultStorer[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
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
        payload: SuccessfulTaskResultToPersist[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
    ) -> SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]:
        return self.store.add_successful_task_result(ctx, self.storer_spec.task_name, payload)


class ExecutorFailureTaskResultStorer[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
    Transform[
        ExecutorFailureTaskResultToPersist[ExecutorPayload],
        ExecutorFailureTaskResult[ExecutorPayload],
    ],
    ActorBuilder,
):
    """Persist one executor-failure task result and trigger retry semantics."""

    executor_failure: Source[ExecutorFailureTaskResult[ExecutorPayload]]
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
        self.executor_failure = self.ok

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks(sinks={SinkName("result-input"): self.sink})

    @override
    def sources(self) -> NodeSources:
        return NodeSources(
            sources={
                SourceName("executor_failure"): self.executor_failure,
                SourceName("error"): self.error,
            },
            default_source=self.executor_failure,
        )

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return ExecutorFailureTaskResultStorerActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class ExecutorFailureTaskResultStorerActor[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
    TransformActor[
        ExecutorFailureTaskResultToPersist[ExecutorPayload],
        ExecutorFailureTaskResult[ExecutorPayload],
    ]
):
    """
    Persist one executor failure, emit it, then signal retry through the error branch.

    Store write failures are surfaced on the error branch instead of being dropped by the runtime loop.
    """

    storer_spec: ExecutorFailureTaskResultStorer[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]
    store: TaskResultStore[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]

    def __init__(
        self,
        spec: ExecutorFailureTaskResultStorer[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.storer_spec = spec
        self.store = spec.task_result_store_provider.get_task_result_store()

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {self.storer_spec.sink: self._handle_executor_failure}

    def _handle_executor_failure(
        self,
        ctx: Context,
        event: ReceiveEvent[ExecutorFailureTaskResultToPersist[ExecutorPayload]],
    ) -> MessagesToSend:
        if event.target != self.storer_spec.sink:
            raise InternalFrameworkException("event target does not match executor failure task result storer sink")

        stored_failure, error = self._process(ctx, event.payload)
        if error is not None:
            return (
                SendEvent(
                    ctx_id=ctx.id,
                    source=self.storer_spec.error,
                    payload=error,
                ),
            )
        if stored_failure is None:
            raise InternalFrameworkException("executor failure task result storer produced no stored result")

        retry_exception = RetryTaskAfterExecutorFailureException()
        retry_exception.__cause__ = stored_failure.executor_failure
        return (
            SendEvent(
                ctx_id=ctx.id,
                source=self.storer_spec.executor_failure,
                payload=stored_failure,
            ),
            SendEvent(
                ctx_id=ctx.id,
                source=self.storer_spec.error,
                payload=retry_exception,
            ),
        )

    @override
    def _transform(
        self,
        ctx: Context,
        payload: ExecutorFailureTaskResultToPersist[ExecutorPayload],
    ) -> ExecutorFailureTaskResult[ExecutorPayload]:
        return self.store.add_executor_failure(ctx, self.storer_spec.task_name, payload)
