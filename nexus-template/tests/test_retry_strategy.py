# pyright: basic

import queue
from datetime import timedelta
from time import monotonic
from typing import Any, override

from pydantic import BaseModel
from utils import CollectorActor, Jobs, empty_context_store, wait_until

from nexus.actors.retry_strategy import Attempt, RetriesExhaustedException, RetryStrategy
from nexus.core.dsl.flow import Flow
from nexus.core.dsl.nodes import Sink, Source
from nexus.core.dsl.piping import Piping
from nexus.core.runtime.actor import Actor, EventHandler
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.context_store_types import ContextId
from nexus.core.runtime.event_bus import EventBus
from nexus.core.runtime.events import MessagesToSend, PipeToBus, ReceiveEvent, SendEvent
from nexus.utils.exceptions import NexusException


class RetryInput(BaseModel):
    value: str


class FailFirstKAttemptsForInputValueActor(Actor):
    def __init__(
        self,
        *,
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
        failing_input_value: str,
        fail_first_k: int,
        name: str = "fail-first-k-for-input-value",
    ) -> None:
        super().__init__(name=name, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.failing_input_value = failing_input_value
        self.fail_first_k = fail_first_k
        self.attempt_sink = Sink[Attempt[RetryInput]](f"{name}-attempt-sink")
        self.failed_source = Source[NexusException](f"{name}-failed-source")
        self.success_source = Source[Attempt[RetryInput]](f"{name}-success-source")
        self.received_attempt_numbers_by_ctx: dict[ContextId, list[int]] = {}
        self.attempt_received_at: dict[tuple[ContextId, int], float] = {}
        self.success_emitted_at_by_ctx: dict[ContextId, float] = {}

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {self.attempt_sink: self._handle_attempt}

    def _handle_attempt(self, _: Context, event: ReceiveEvent[Attempt[RetryInput]]) -> MessagesToSend:
        attempt_number = int(event.payload.attempt_number)
        self.received_attempt_numbers_by_ctx.setdefault(event.ctx_id, []).append(attempt_number)
        self.attempt_received_at[(event.ctx_id, attempt_number)] = monotonic()

        should_fail = (
            event.payload.original_input.value == self.failing_input_value and attempt_number <= self.fail_first_k
        )
        if should_fail:
            return SendEvent(
                ctx_id=event.ctx_id,
                source=self.failed_source,
                payload=NexusException(f"failed attempt #{attempt_number}"),
            )
        else:
            self.success_emitted_at_by_ctx[event.ctx_id] = monotonic()
            return SendEvent(
                ctx_id=event.ctx_id,
                source=self.success_source,
                payload=event.payload,
            )


def test_retry_strategy_actor_sends_first_attempt_downstream_on_input() -> None:
    context_store = empty_context_store()
    pipe_to_bus: PipeToBus = queue.Queue()

    retry_strategy = RetryStrategy[RetryInput]("retry-strategy", max_attempts=3, delay=timedelta(milliseconds=10))
    retry_actor = retry_strategy.build_actor(pipe_to_bus=pipe_to_bus, context_store=context_store)
    collector = CollectorActor[Attempt[RetryInput]](
        pipe_to_bus=pipe_to_bus,
        context_store=context_store,
        name="attempt-collector",
    )

    upstream_source = Source[RetryInput]("retry-input-source")
    piping = Piping()
    piping.add_flow(Flow.from_connectable(upstream_source).then(retry_strategy.input))
    piping.add_flow(Flow.from_connectable(retry_strategy.next_attempt).then(collector.sink))

    event_bus = EventBus(
        connections=piping.pipes,
        input_pipe=pipe_to_bus,
        actors=[retry_actor, collector],
        context_store=context_store,
    )

    with context_store.create_context() as context:
        ctx_id = context.id

    payload = RetryInput(value="ping")
    jobs = Jobs(event_bus.run_loop(), retry_actor.run_loop(), collector.run_loop())
    try:
        pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=upstream_source, payload=payload))

        wait_until(lambda: len(collector.received_events) == 1)

        received = collector.received_events[0]
        assert received.ctx_id == ctx_id
        assert received.payload.attempt_number == 1
        assert received.payload.original_input == payload
    finally:
        event_bus.request_stop()
        jobs.join()


def test_retry_strategy_actor_retries_first_k_failures() -> None:
    context_store = empty_context_store()
    pipe_to_bus: PipeToBus = queue.Queue()

    retry_strategy = RetryStrategy[RetryInput]("retry-strategy", max_attempts=5, delay=timedelta(milliseconds=10))
    retry_actor = retry_strategy.build_actor(pipe_to_bus=pipe_to_bus, context_store=context_store)
    flaky = FailFirstKAttemptsForInputValueActor(
        pipe_to_bus=pipe_to_bus,
        context_store=context_store,
        failing_input_value="needs-retries",
        fail_first_k=2,
    )

    upstream_source = Source[RetryInput]("retry-input-source")
    piping = Piping()
    piping.add_flow(Flow.from_connectable(upstream_source).then(retry_strategy.input))
    piping.add_flow(Flow.from_connectable(retry_strategy.next_attempt).then(flaky.attempt_sink))
    piping.add_flow(Flow.from_connectable(flaky.failed_source).then(retry_strategy.failed_attempt))

    event_bus = EventBus(
        connections=piping.pipes,
        input_pipe=pipe_to_bus,
        actors=[retry_actor, flaky],
        context_store=context_store,
    )

    with context_store.create_context() as context:
        ctx_id = context.id

    payload = RetryInput(value="needs-retries")
    jobs = Jobs(event_bus.run_loop(), retry_actor.run_loop(), flaky.run_loop())
    try:
        pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=upstream_source, payload=payload))

        wait_until(lambda: len(flaky.received_attempt_numbers_by_ctx.get(ctx_id, [])) >= 3)

        assert flaky.received_attempt_numbers_by_ctx[ctx_id] == [1, 2, 3]
    finally:
        event_bus.request_stop()
        jobs.join()


def test_retry_strategy_actor_emits_retries_exhausted_after_too_many_failures() -> None:
    context_store = empty_context_store()
    pipe_to_bus: PipeToBus = queue.Queue()

    retry_strategy = RetryStrategy[RetryInput]("retry-strategy", max_attempts=3, delay=timedelta(milliseconds=10))
    retry_actor = retry_strategy.build_actor(pipe_to_bus=pipe_to_bus, context_store=context_store)
    flaky = FailFirstKAttemptsForInputValueActor(
        pipe_to_bus=pipe_to_bus,
        context_store=context_store,
        failing_input_value="always-fails",
        fail_first_k=10,
    )
    exhausted_collector = CollectorActor[RetriesExhaustedException](
        pipe_to_bus=pipe_to_bus,
        context_store=context_store,
        name="retries-exhausted-collector",
    )

    upstream_source = Source[RetryInput]("retry-input-source")
    piping = Piping()
    piping.add_flow(Flow.from_connectable(upstream_source).then(retry_strategy.input))
    piping.add_flow(Flow.from_connectable(retry_strategy.next_attempt).then(flaky.attempt_sink))
    piping.add_flow(Flow.from_connectable(flaky.failed_source).then(retry_strategy.failed_attempt))
    piping.add_flow(Flow.from_connectable(retry_strategy.error).then(exhausted_collector.sink))

    event_bus = EventBus(
        connections=piping.pipes,
        input_pipe=pipe_to_bus,
        actors=[retry_actor, flaky, exhausted_collector],
        context_store=context_store,
    )

    with context_store.create_context() as context:
        ctx_id = context.id

    payload = RetryInput(value="always-fails")
    jobs = Jobs(event_bus.run_loop(), retry_actor.run_loop(), flaky.run_loop(), exhausted_collector.run_loop())
    try:
        pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=upstream_source, payload=payload))

        wait_until(lambda: len(exhausted_collector.received_events) == 1)

        assert flaky.received_attempt_numbers_by_ctx[ctx_id] == [1, 2, 3]
        exhausted_event = exhausted_collector.received_events[0]
        assert exhausted_event.ctx_id == ctx_id
        assert isinstance(exhausted_event.payload, RetriesExhaustedException)
        assert "All 3 retry attempts exhausted" in str(exhausted_event.payload)
    finally:
        event_bus.request_stop()
        jobs.join()


def test_retry_strategy_retry_wait_in_one_context_does_not_block_other_context() -> None:
    context_store = empty_context_store()
    pipe_to_bus: PipeToBus = queue.Queue()

    retry_strategy = RetryStrategy[RetryInput]("retry-strategy", max_attempts=5, delay=timedelta(milliseconds=200))
    retry_actor = retry_strategy.build_actor(pipe_to_bus=pipe_to_bus, context_store=context_store)
    flaky = FailFirstKAttemptsForInputValueActor(
        pipe_to_bus=pipe_to_bus,
        context_store=context_store,
        failing_input_value="needs-retries",
        fail_first_k=2,
    )
    success_collector = CollectorActor[Attempt[RetryInput]](
        pipe_to_bus=pipe_to_bus,
        context_store=context_store,
        name="attempt-collector-success",
    )

    upstream_source = Source[RetryInput]("retry-input-source-multi-context")
    piping = Piping()
    piping.add_flow(Flow.from_connectable(upstream_source).then(retry_strategy.input))
    piping.add_flow(Flow.from_connectable(retry_strategy.next_attempt).then(flaky.attempt_sink))
    piping.add_flow(Flow.from_connectable(flaky.failed_source).then(retry_strategy.failed_attempt))
    piping.add_flow(Flow.from_connectable(flaky.success_source).then(success_collector.sink))

    event_bus = EventBus(
        connections=piping.pipes,
        input_pipe=pipe_to_bus,
        actors=[retry_actor, flaky, success_collector],
        context_store=context_store,
    )

    with context_store.create_context() as context:
        ctx_needs_retries = context.id
    with context_store.create_context() as context:
        ctx_always_success = context.id

    payload_needs_retries = RetryInput(value="needs-retries")
    payload_always_success = RetryInput(value="always-success")

    jobs = Jobs(event_bus.run_loop(), retry_actor.run_loop(), flaky.run_loop(), success_collector.run_loop())
    try:
        pipe_to_bus.put(SendEvent(ctx_id=ctx_needs_retries, source=upstream_source, payload=payload_needs_retries))
        wait_until(lambda: flaky.received_attempt_numbers_by_ctx.get(ctx_needs_retries) == [1])

        pipe_to_bus.put(SendEvent(ctx_id=ctx_always_success, source=upstream_source, payload=payload_always_success))
        wait_until(lambda: ctx_always_success in flaky.success_emitted_at_by_ctx)
        wait_until(lambda: (ctx_needs_retries, 2) in flaky.attempt_received_at)

        assert flaky.success_emitted_at_by_ctx[ctx_always_success] < flaky.attempt_received_at[(ctx_needs_retries, 2)]

        wait_until(
            lambda: {event.ctx_id for event in success_collector.received_events}
            >= {ctx_needs_retries, ctx_always_success}
        )

        assert flaky.received_attempt_numbers_by_ctx[ctx_needs_retries] == [1, 2, 3]
        assert flaky.received_attempt_numbers_by_ctx[ctx_always_success] == [1]

        successes_by_ctx = {event.ctx_id: event.payload for event in success_collector.received_events}
        assert successes_by_ctx[ctx_always_success].attempt_number == 1
        assert successes_by_ctx[ctx_always_success].original_input == payload_always_success
        assert successes_by_ctx[ctx_needs_retries].attempt_number == 3
        assert successes_by_ctx[ctx_needs_retries].original_input == payload_needs_retries
    finally:
        event_bus.request_stop()
        jobs.join()
