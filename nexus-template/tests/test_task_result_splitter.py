# pyright: basic

from utils import (
    CollectorActor,
    InMemoryTestTaskResultStoreProvider,
    build_nexus_task_result,
    store_nexus_task_result,
    wait_until,
)

from nexus.actors.task_result_splitter import TaskResultSplitter
from nexus.core.dsl.flow import Flow
from nexus.core.dsl.nodes import NodeId, Source
from nexus.core.runtime.context_store import ContextStore, InMemoryContextStorePersistence
from nexus.core.runtime.context_store_types import ChildContextCreated, ContextCreated
from nexus.core.runtime.events import SendEvent
from nexus.core.runtime.nexus_task_types import NexusTaskName
from nexus.core.runtime.subnet_runtime import SubnetBuilder
from nexus.core.runtime.task_result_store import SingleTaskResult
from nexus.utils.exceptions import ExecutorFailureException, NexusException

type DummyExecutorPayload = str
type DummyExecutorOutput = int
type DummyExecutorPublicResult = str


def _context_store_with_persistence() -> tuple[ContextStore, InMemoryContextStorePersistence]:
    persistence = InMemoryContextStorePersistence()
    context_store = ContextStore.recover_from(persistence).context_store
    return context_store, persistence


def test_task_result_splitter_emits_converted_executor_output_with_parent_context_and_task_result_with_child_context() -> None:
    context_store, persistence = _context_store_with_persistence()
    converted_payloads: list[SingleTaskResult[DummyExecutorPayload, DummyExecutorOutput]] = []

    splitter = TaskResultSplitter[
        DummyExecutorPayload,
        DummyExecutorOutput,
        DummyExecutorPublicResult,
    ](
        NodeId("task-result-splitter"),
        executor_result_converter=lambda task_result: (
            converted_payloads.append(task_result) or f"public-{task_result.executor_output}"
        ),
    )
    builder = SubnetBuilder(nodes=[splitter], context_store=context_store)

    upstream_source = Source[SingleTaskResult[DummyExecutorPayload, DummyExecutorOutput]]("task-result-upstream")
    task_result_collector = CollectorActor[SingleTaskResult[DummyExecutorPayload, DummyExecutorOutput]](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=context_store,
        name="task-result-collector",
    )
    executor_output_collector = CollectorActor[DummyExecutorPublicResult | NexusException](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=context_store,
        name="executor-output-collector",
    )

    runtime = (
        builder.add_flows(
            Flow.from_connectable(upstream_source).then(splitter.task_result_input),
            Flow.from_connectable(splitter.task_result).then(task_result_collector.sink),
            Flow.from_connectable(splitter.executor_output).then(executor_output_collector.sink),
        )
        .add_actors(task_result_collector, executor_output_collector)
        .build()
    )

    store_provider = InMemoryTestTaskResultStoreProvider[DummyExecutorPayload, DummyExecutorOutput]()
    task_result = store_nexus_task_result(
        context_store=context_store,
        task_result_store=store_provider.get_task_result_store(),
        task_name=NexusTaskName("task-result-splitter-test-task"),
        result=build_nexus_task_result(
            executor_payload="payload",
            output=7,
            block_number=42,
            target_hotkey="task-result-splitter-neuron",
        ),
    )
    with context_store.create_context() as context:
        parent_ctx_id = context.id

    with runtime.running(shutdown_timeout_seconds=1.0):
        runtime.pipe_to_bus.put(SendEvent(ctx_id=parent_ctx_id, source=upstream_source, payload=task_result))
        wait_until(
            lambda: len(task_result_collector.received_events) == 1
            and len(executor_output_collector.received_events) == 1
        )

    task_result_event = task_result_collector.received_events[0]
    executor_output_event = executor_output_collector.received_events[0]
    child_ctx_id = task_result_event.ctx_id

    assert task_result_event.payload == task_result
    assert task_result_event.ctx_id != parent_ctx_id

    assert executor_output_event.payload == "public-7"
    assert executor_output_event.ctx_id == parent_ctx_id
    assert converted_payloads == [task_result]

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


def test_task_result_splitter_emits_failure_without_conversion() -> None:
    context_store, _ = _context_store_with_persistence()
    converter_calls = 0

    def converter(_: SingleTaskResult[DummyExecutorPayload, DummyExecutorOutput]) -> DummyExecutorPublicResult:
        nonlocal converter_calls
        converter_calls += 1
        return "unexpected"

    splitter = TaskResultSplitter[
        DummyExecutorPayload,
        DummyExecutorOutput,
        DummyExecutorPublicResult,
    ](
        NodeId("task-result-splitter"),
        executor_result_converter=converter,
    )
    builder = SubnetBuilder(nodes=[splitter], context_store=context_store)

    upstream_source = Source[SingleTaskResult[DummyExecutorPayload, DummyExecutorOutput]]("task-result-upstream")
    task_result_collector = CollectorActor[SingleTaskResult[DummyExecutorPayload, DummyExecutorOutput]](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=context_store,
        name="task-result-collector",
    )
    executor_output_collector = CollectorActor[DummyExecutorPublicResult | NexusException](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=context_store,
        name="executor-output-collector",
    )

    runtime = (
        builder.add_flows(
            Flow.from_connectable(upstream_source).then(splitter.task_result_input),
            Flow.from_connectable(splitter.task_result).then(task_result_collector.sink),
            Flow.from_connectable(splitter.executor_output).then(executor_output_collector.sink),
        )
        .add_actors(task_result_collector, executor_output_collector)
        .build()
    )

    store_provider = InMemoryTestTaskResultStoreProvider[DummyExecutorPayload, DummyExecutorOutput]()
    task_result = store_nexus_task_result(
        context_store=context_store,
        task_result_store=store_provider.get_task_result_store(),
        task_name=NexusTaskName("task-result-splitter-test-task"),
        result=build_nexus_task_result(
            executor_payload="payload",
            output=ExecutorFailureException(NexusException("boom")),
            block_number=42,
            target_hotkey="task-result-splitter-neuron",
        ),
    )
    with context_store.create_context() as context:
        parent_ctx_id = context.id

    with runtime.running(shutdown_timeout_seconds=1.0):
        runtime.pipe_to_bus.put(SendEvent(ctx_id=parent_ctx_id, source=upstream_source, payload=task_result))
        wait_until(
            lambda: len(task_result_collector.received_events) == 1
            and len(executor_output_collector.received_events) == 1
        )

    task_result_event = task_result_collector.received_events[0]
    executor_output_event = executor_output_collector.received_events[0]

    assert task_result_event.payload == task_result
    assert task_result_event.ctx_id != parent_ctx_id
    assert isinstance(executor_output_event.payload, ExecutorFailureException)
    assert executor_output_event.ctx_id == parent_ctx_id
    assert converter_calls == 0


def test_task_result_splitter_drops_events_when_converter_raises() -> None:
    context_store, persistence = _context_store_with_persistence()

    def converter(_: SingleTaskResult[DummyExecutorPayload, DummyExecutorOutput]) -> DummyExecutorPublicResult:
        raise ValueError("conversion failed")

    splitter = TaskResultSplitter[
        DummyExecutorPayload,
        DummyExecutorOutput,
        DummyExecutorPublicResult,
    ](
        NodeId("task-result-splitter"),
        executor_result_converter=converter,
    )
    builder = SubnetBuilder(nodes=[splitter], context_store=context_store)

    upstream_source = Source[SingleTaskResult[DummyExecutorPayload, DummyExecutorOutput]]("task-result-upstream")
    task_result_collector = CollectorActor[SingleTaskResult[DummyExecutorPayload, DummyExecutorOutput]](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=context_store,
        name="task-result-collector",
    )
    executor_output_collector = CollectorActor[DummyExecutorPublicResult | NexusException](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=context_store,
        name="executor-output-collector",
    )

    runtime = (
        builder.add_flows(
            Flow.from_connectable(upstream_source).then(splitter.task_result_input),
            Flow.from_connectable(splitter.task_result).then(task_result_collector.sink),
            Flow.from_connectable(splitter.executor_output).then(executor_output_collector.sink),
        )
        .add_actors(task_result_collector, executor_output_collector)
        .build()
    )

    store_provider = InMemoryTestTaskResultStoreProvider[DummyExecutorPayload, DummyExecutorOutput]()
    task_result = store_nexus_task_result(
        context_store=context_store,
        task_result_store=store_provider.get_task_result_store(),
        task_name=NexusTaskName("task-result-splitter-test-task"),
        result=build_nexus_task_result(
            executor_payload="payload",
            output=7,
            block_number=42,
            target_hotkey="task-result-splitter-neuron",
        ),
    )
    with context_store.create_context() as context:
        parent_ctx_id = context.id

    with runtime.running(shutdown_timeout_seconds=1.0):
        runtime.pipe_to_bus.put(SendEvent(ctx_id=parent_ctx_id, source=upstream_source, payload=task_result))
        wait_until(
            lambda: any(
                isinstance(entry.data, ContextCreated)
                and entry.ctx != parent_ctx_id
                and entry.data.parents == (parent_ctx_id,)
                for entry in persistence.log_entries()
            )
        )

    assert task_result_collector.received_events == []
    assert executor_output_collector.received_events == []
