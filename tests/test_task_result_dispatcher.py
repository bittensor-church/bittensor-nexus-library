# pyright: basic

from utils import (
    CollectorActor,
    InMemoryTestTaskResultStoreProvider,
    build_nexus_task_result,
    store_executor_failure_task_result,
    store_successful_task_result,
    wait_until,
)

from nexus.v1 import (
    ChildContextCreated,
    ContextCreated,
    ContextStore,
    ExecutorFailureException,
    ExecutorFailureTaskResult,
    Flow,
    InMemoryContextStorePersistence,
    NexusException,
    NexusTaskName,
    NodeId,
    SendEvent,
    Source,
    SubnetBuilder,
    SuccessfulTaskResult,
    TaskResultDispatcher,
)

type DummyExecutorPayload = str
type DummyExecutorOutput = int
type DummyExecutorPublicOutput = str


def _context_store_with_persistence() -> tuple[ContextStore, InMemoryContextStorePersistence]:
    persistence = InMemoryContextStorePersistence()
    context_store = ContextStore.recover_from(persistence).context_store
    return context_store, persistence


def test_task_result_dispatcher_emits_successful_task_result_on_child_context_and_executor_output_on_parent() -> None:
    context_store, persistence = _context_store_with_persistence()
    dispatcher = TaskResultDispatcher[
        DummyExecutorPayload,
        DummyExecutorOutput,
        DummyExecutorPublicOutput,
    ](NodeId("task-result-dispatcher"))
    builder = SubnetBuilder(nodes=[dispatcher], context_store=context_store)

    upstream_source = Source[
        SuccessfulTaskResult[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorPublicOutput]
    ]("successful-task-result-upstream")
    successful_task_result_collector = CollectorActor[
        SuccessfulTaskResult[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorPublicOutput]
    ](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=context_store,
        name="successful-task-result-collector",
    )
    executor_failure_collector = CollectorActor[ExecutorFailureTaskResult[DummyExecutorPayload]](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=context_store,
        name="executor-failure-collector",
    )
    executor_output_collector = CollectorActor[DummyExecutorPublicOutput](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=context_store,
        name="executor-output-collector",
    )

    runtime = (
        builder.add_flows(
            Flow.from_connectable(upstream_source).then(dispatcher.successful_task_result_input),
            Flow.from_connectable(dispatcher.successful_task_result).then(successful_task_result_collector.sink),
            Flow.from_connectable(dispatcher.executor_failure).then(executor_failure_collector.sink),
            Flow.from_connectable(dispatcher.executor_output).then(executor_output_collector.sink),
        )
        .add_actors(successful_task_result_collector, executor_failure_collector, executor_output_collector)
        .build()
    )

    store_provider = InMemoryTestTaskResultStoreProvider[
        DummyExecutorPayload,
        DummyExecutorOutput,
        DummyExecutorPublicOutput,
    ]()
    successful_task_result = store_successful_task_result(
        context_store=context_store,
        task_result_store=store_provider.get_task_result_store(),
        task_name=NexusTaskName("task-result-dispatcher-test-task"),
        result=build_nexus_task_result(
            executor_payload="payload",
            output=7,
            block_number=42,
            target_hotkey="task-result-dispatcher-neuron",
        ),
        executor_public_output="public-7",
    )
    with context_store.create_context() as context:
        parent_ctx_id = context.id

    with runtime.running(shutdown_timeout_seconds=1.0):
        runtime.pipe_to_bus.put(
            SendEvent(
                ctx_id=parent_ctx_id,
                source=upstream_source,
                payload=successful_task_result,
            )
        )
        wait_until(
            lambda: (
                len(successful_task_result_collector.received_events) == 1
                and len(executor_output_collector.received_events) == 1
            )
        )

    assert len(executor_failure_collector.received_events) == 0

    successful_task_result_event = successful_task_result_collector.received_events[0]
    executor_output_event = executor_output_collector.received_events[0]
    child_ctx_id = successful_task_result_event.ctx_id

    assert successful_task_result_event.payload == successful_task_result
    assert successful_task_result_event.ctx_id != parent_ctx_id
    assert executor_output_event.payload == "public-7"
    assert executor_output_event.ctx_id == parent_ctx_id

    with context_store.get_context(child_ctx_id):
        pass

    child_creation_entry = next(
        (
            entry
            for entry in persistence.log_entries()
            if entry.ctx == child_ctx_id and isinstance(entry.data, ContextCreated)
        ),
        None,
    )
    assert child_creation_entry is not None
    assert isinstance(child_creation_entry.data, ContextCreated)
    assert child_creation_entry.data.parents == (parent_ctx_id,)

    child_relation_exists = any(
        entry.ctx == parent_ctx_id
        and isinstance(entry.data, ChildContextCreated)
        and entry.data.child_ctx == child_ctx_id
        for entry in persistence.log_entries()
    )
    assert child_relation_exists


def test_task_result_dispatcher_emits_executor_failure_on_child_context_without_executor_output() -> None:
    context_store, persistence = _context_store_with_persistence()
    dispatcher = TaskResultDispatcher[
        DummyExecutorPayload,
        DummyExecutorOutput,
        DummyExecutorPublicOutput,
    ](NodeId("task-result-dispatcher"))
    builder = SubnetBuilder(nodes=[dispatcher], context_store=context_store)

    upstream_source = Source[ExecutorFailureTaskResult[DummyExecutorPayload]]("executor-failure-upstream")
    successful_task_result_collector = CollectorActor[
        SuccessfulTaskResult[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorPublicOutput]
    ](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=context_store,
        name="successful-task-result-collector",
    )
    executor_failure_collector = CollectorActor[ExecutorFailureTaskResult[DummyExecutorPayload]](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=context_store,
        name="executor-failure-collector",
    )
    executor_output_collector = CollectorActor[DummyExecutorPublicOutput](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=context_store,
        name="executor-output-collector",
    )

    runtime = (
        builder.add_flows(
            Flow.from_connectable(upstream_source).then(dispatcher.executor_failure_input),
            Flow.from_connectable(dispatcher.successful_task_result).then(successful_task_result_collector.sink),
            Flow.from_connectable(dispatcher.executor_failure).then(executor_failure_collector.sink),
            Flow.from_connectable(dispatcher.executor_output).then(executor_output_collector.sink),
        )
        .add_actors(successful_task_result_collector, executor_failure_collector, executor_output_collector)
        .build()
    )

    store_provider = InMemoryTestTaskResultStoreProvider[
        DummyExecutorPayload,
        DummyExecutorOutput,
        DummyExecutorPublicOutput,
    ]()
    executor_failure = store_executor_failure_task_result(
        context_store=context_store,
        task_result_store=store_provider.get_task_result_store(),
        task_name=NexusTaskName("task-result-dispatcher-test-task"),
        result=build_nexus_task_result(
            executor_payload="payload",
            output=ExecutorFailureException(NexusException("boom")),
            block_number=42,
            target_hotkey="task-result-dispatcher-neuron",
        ),
    )
    with context_store.create_context() as context:
        parent_ctx_id = context.id

    with runtime.running(shutdown_timeout_seconds=1.0):
        runtime.pipe_to_bus.put(
            SendEvent(
                ctx_id=parent_ctx_id,
                source=upstream_source,
                payload=executor_failure,
            )
        )
        wait_until(lambda: len(executor_failure_collector.received_events) == 1)

    assert len(successful_task_result_collector.received_events) == 0
    assert len(executor_output_collector.received_events) == 0

    executor_failure_event = executor_failure_collector.received_events[0]
    child_ctx_id = executor_failure_event.ctx_id

    assert executor_failure_event.payload == executor_failure
    assert executor_failure_event.ctx_id != parent_ctx_id

    with context_store.get_context(child_ctx_id):
        pass

    child_creation_entry = next(
        (
            entry
            for entry in persistence.log_entries()
            if entry.ctx == child_ctx_id and isinstance(entry.data, ContextCreated)
        ),
        None,
    )
    assert child_creation_entry is not None
    assert isinstance(child_creation_entry.data, ContextCreated)
    assert child_creation_entry.data.parents == (parent_ctx_id,)
