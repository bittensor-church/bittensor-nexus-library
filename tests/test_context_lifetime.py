# pyright: basic

import logging
import queue
import threading
import time
from collections.abc import Callable, Generator
from typing import Any, override

import pytest
from utils import CollectorActor, Jobs, wait_until

from nexus.v1 import (
    Actor,
    ActorBuilder,
    Context,
    ContextCompleted,
    ContextId,
    ContextStore,
    EventBus,
    EventHandler,
    Flow,
    InMemoryContextStorePersistence,
    InternalFrameworkException,
    InvalidContextIdException,
    MessagesToSend,
    PipeToBus,
    Piping,
    Producer,
    ProducerActor,
    ReceiveEvent,
    ReleaseContextEvent,
    SendEvent,
    Sink,
    Source,
    StopActorEvent,
    StopBusEvent,
    Stringify,
    SubnetBuilder,
)


class KeepingPersistence(InMemoryContextStorePersistence):
    """Keeps released contexts' entries, like a durable persistence that does not compact."""

    @override
    def release_context(self, ctx: ContextId) -> None:
        pass


class Ticks(Producer[int], ActorBuilder):
    """Clock-like producer; every tick gets a new context."""

    def __init__(self, count: int, before_tick: Callable[[int], None]) -> None:
        super().__init__("ticks")
        self.count = count
        self.before_tick = before_tick

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return TicksActor(self, pipe_to_bus, context_store)


class TicksActor(ProducerActor[int]):
    def __init__(self, spec: Ticks, pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(spec, pipe_to_bus, context_store)
        self.ticks = spec

    @override
    def _produce(self) -> Generator[int]:
        for tick in range(self.ticks.count):
            self.ticks.before_tick(tick)
            yield tick


class CountingActor(Actor):
    """Counts deliveries without keeping their contexts; optionally fails every handled message."""

    def __init__(self, *, name: str, pipe_to_bus: PipeToBus, context_store: ContextStore, fail: bool = False) -> None:
        super().__init__(name=name, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.sink = Sink[Any](f"{name}-sink")
        self.fail = fail
        self.handled = 0

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {self.sink: self._handle}

    def _handle(self, _: Context, __: ReceiveEvent[Any]) -> MessagesToSend:
        self.handled += 1
        if self.fail:
            raise ValueError("handler failure")
        return ()


class EmitLaterActor(Actor):
    """Retains each received context and emits on it later from another thread, like a timer or pending request."""

    def __init__(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(name="emit-later", pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.sink = Sink[str]("emit-later-sink")
        self.source = Source[str]("emit-later-source")
        self.retained: queue.Queue[ContextId] = queue.Queue()

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {self.sink: self._handle}

    def _handle(self, ctx: Context, _: ReceiveEvent[str]) -> MessagesToSend:
        self.context_store.retain(ctx.id)
        self.retained.put(ctx.id)
        return ()

    def emit(self, payload: str) -> None:
        ctx_id = self.retained.get(timeout=1.0)
        self._pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=self.source, payload=payload))
        self._pipe_to_bus.put(ReleaseContextEvent(ctx_id))


def _store(persistence: InMemoryContextStorePersistence | None = None) -> ContextStore:
    return ContextStore.recover_from(persistence or InMemoryContextStorePersistence()).context_store


def _create(context_store: ContextStore, *parents: ContextId) -> ContextId:
    with context_store.create_context(parents=parents) as context:
        return context.id


def test_clock_with_taps_keeps_contexts_and_entries_bounded() -> None:
    """Regression: every tick and every tap child context used to stay in memory forever."""
    tick_count = 2_000
    window = 10
    persistence = InMemoryContextStorePersistence()
    store = _store(persistence)
    peak_live_contexts = 0

    def before_tick(tick: int) -> None:
        # pace the clock like a real one: stay at most `window` ticks ahead of the slowest consumer
        nonlocal peak_live_contexts
        while tick - min(c.handled for c in consumers) > window:
            time.sleep(0.001)
        peak_live_contexts = max(peak_live_contexts, store.live_context_count)

    ticks = Ticks(tick_count, before_tick)
    stringify = Stringify("tick-stringify")
    builder = SubnetBuilder(nodes=[ticks, stringify], context_store=store)
    observer = CountingActor(name="observer", pipe_to_bus=builder.pipe_to_bus, context_store=builder.context_store)
    failing = CountingActor(
        name="failing", pipe_to_bus=builder.pipe_to_bus, context_store=builder.context_store, fail=True
    )
    stringified = CountingActor(
        name="stringified", pipe_to_bus=builder.pipe_to_bus, context_store=builder.context_store
    )
    runtime = (
        builder.add_flows(
            Flow.from_connectable(ticks.source).then(taps=[stringify.sink, observer.sink, failing.sink]),
            Flow.from_connectable(stringify.ok).then(stringified.sink),
        )
        .add_actors(observer, failing, stringified)
        .build()
    )
    consumers = (observer, failing, stringified)

    with runtime.running():
        wait_until(
            lambda: all(c.handled == tick_count for c in consumers) and store.live_context_count == 0,
            timeout=60.0,
        )

    assert len(persistence.log_entries()) == 0
    # live contexts follow in-flight work; they used to grow by 4 per tick (a tick and its 3 tap children)
    assert peak_live_contexts <= 8 * window


def test_released_context_is_removed_and_its_entries_dropped() -> None:
    persistence = InMemoryContextStorePersistence()
    store = _store(persistence)
    ctx_id = _create(store)
    store.retain(ctx_id)

    store._release(ctx_id)

    assert store.live_context_count == 0
    assert len(persistence.log_entries()) == 0
    with pytest.raises(InvalidContextIdException, match="released"):
        with store.get_context(ctx_id):
            pass
    with pytest.raises(InvalidContextIdException, match="released"):
        store.retain(ctx_id)


def test_child_of_released_parent_fails_clearly() -> None:
    store = _store()
    parent = _create(store)
    store.retain(parent)
    store._release(parent)

    with pytest.raises(InvalidContextIdException, match=f"Context {parent} not found.*released"):
        _create(store, parent)


def test_children_outlive_their_released_parent() -> None:
    persistence = InMemoryContextStorePersistence()
    store = _store(persistence)
    parent = _create(store)
    with store.get_context(parent) as context:
        context.set_user_data("key", "parent value")
    child = _create(store, parent)
    store.retain(parent)
    store.retain(child)

    store._release(parent)

    with store.get_context(child) as context:
        assert context.copy_user_data() == {"key": "parent value"}
        assert context.copy_parent_context_snapshots()[parent].user_data == {"key": "parent value"}
    store._release(child)
    assert len(persistence.log_entries()) == 0


def test_context_lives_until_its_last_reference_is_released() -> None:
    store = _store()
    ctx_id = _create(store)
    store.retain(ctx_id)
    store.retain(ctx_id)

    store._release(ctx_id)
    assert store.live_context_count == 1
    store._release(ctx_id)
    assert store.live_context_count == 0


def test_release_without_reference_raises() -> None:
    store = _store()
    ctx_id = _create(store)

    with pytest.raises(InternalFrameworkException, match="released more often"):
        store._release(ctx_id)
    assert store.live_context_count == 1


def test_released_context_is_completed_for_durable_recovery() -> None:
    persistence = KeepingPersistence()
    store = _store(persistence)
    released = _create(store)
    in_flight = _create(store)
    store.retain(released)
    store.retain(in_flight)

    store._release(released)

    assert isinstance(persistence.log_entries()[-1].data, ContextCompleted)
    recovered = ContextStore.recover_from(persistence).context_store
    assert recovered.live_context_count == 1
    with recovered.get_context(in_flight) as context:
        assert context.id == in_flight


def test_actor_releases_delivery_after_its_messages_also_when_handler_fails() -> None:
    store = _store()
    pipe_to_bus: PipeToBus = queue.Queue()
    failing = CountingActor(name="failing", pipe_to_bus=pipe_to_bus, context_store=store, fail=True)
    ctx_id = _create(store)

    failing.pipe_from_bus.put(ReceiveEvent(ctx_id=ctx_id, target=failing.sink, payload="boom"))
    failing.pipe_from_bus.put(StopActorEvent())
    Jobs(failing.run_loop()).join()

    assert failing.handled == 1
    assert pipe_to_bus.get_nowait() == ReleaseContextEvent(ctx_id)
    # the stop signal belongs to no context and is not released
    assert pipe_to_bus.empty()


def test_retained_context_survives_handler_until_released_after_last_message() -> None:
    store = _store()
    pipe_to_bus: PipeToBus = queue.Queue()
    source = Source[str]("source")
    holder = EmitLaterActor(pipe_to_bus=pipe_to_bus, context_store=store)
    collector = CollectorActor[str](pipe_to_bus=pipe_to_bus, context_store=store)
    piping = Piping()
    piping.add_flow(Flow.from_connectable(source).then(holder.sink))
    piping.add_flow(Flow.from_connectable(holder.source).then(collector.sink))
    event_bus = EventBus(
        connections=piping.pipes, input_pipe=pipe_to_bus, actors=[holder, collector], context_store=store
    )
    ctx_id = _create(store)

    jobs = Jobs(event_bus.run_loop(), holder.run_loop(), collector.run_loop())
    pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=source, payload="request"))
    emitter = threading.Thread(target=holder.emit, args=("late reply",))
    emitter.start()
    emitter.join(1.0)

    wait_until(lambda: len(collector.received_events) == 1)
    assert collector.received_events[0].ctx_id == ctx_id
    collector.release_received()
    wait_until(lambda: store.live_context_count == 0)

    event_bus.request_stop()
    jobs.join()


def test_bus_survives_unbalanced_release_and_messages_for_released_contexts(caplog: pytest.LogCaptureFixture) -> None:
    store = _store()
    pipe_to_bus: PipeToBus = queue.Queue()
    source = Source[str]("source")
    collector = CollectorActor[str](pipe_to_bus=pipe_to_bus, context_store=store)
    piping = Piping()
    piping.add_flow(Flow.from_connectable(source).then(collector.sink))
    event_bus = EventBus(connections=piping.pipes, input_pipe=pipe_to_bus, actors=[collector], context_store=store)
    unreferenced = _create(store)
    released = _create(store)
    store.retain(released)
    store._release(released)
    valid = _create(store)

    with caplog.at_level(logging.ERROR):
        pipe_to_bus.put(ReleaseContextEvent(unreferenced))
        pipe_to_bus.put(SendEvent(ctx_id=released, source=source, payload="dropped"))
        pipe_to_bus.put(SendEvent(ctx_id=valid, source=source, payload="delivered"))
        jobs = Jobs(event_bus.run_loop(), collector.run_loop())
        wait_until(lambda: len(collector.received_events) == 1)

    assert collector.received_events[0].payload == "delivered"
    assert any("Failed to release context" in record.message for record in caplog.records)
    assert any("Dropping message" in record.message for record in caplog.records)
    event_bus.request_stop()
    jobs.join()


def test_undelivered_messages_keep_their_contexts_when_the_bus_stops() -> None:
    store = _store()
    pipe_to_bus: PipeToBus = queue.Queue()
    source = Source[str]("source")
    idle = CountingActor(name="idle", pipe_to_bus=pipe_to_bus, context_store=store)
    piping = Piping()
    piping.add_flow(Flow.from_connectable(source).then(idle.sink))
    event_bus = EventBus(connections=piping.pipes, input_pipe=pipe_to_bus, actors=[idle], context_store=store)
    ctx_id = _create(store)

    pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=source, payload="pending"))
    pipe_to_bus.put(StopBusEvent())
    Jobs(event_bus.run_loop()).join()

    # the queued delivery still references the context, so it stays recoverable
    assert store.live_context_count == 1
    assert isinstance(idle.pipe_from_bus.get_nowait(), ReceiveEvent)
    assert isinstance(idle.pipe_from_bus.get_nowait(), StopActorEvent)
