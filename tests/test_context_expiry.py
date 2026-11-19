# pyright: basic

import logging
import queue
import threading
from datetime import timedelta
from typing import override

import pytest
from utils import CollectorActor, Jobs, wait_until

from nexus.v1 import (
    ContextCompleted,
    ContextId,
    ContextStore,
    EventBus,
    Flow,
    InMemoryContextStorePersistence,
    InvalidContextIdException,
    PipeToBus,
    Piping,
    SendEvent,
    Source,
    StopBusEvent,
    SubnetBuilder,
)

TTL = timedelta(minutes=10)


class FakeClock:
    def __init__(self) -> None:
        self.now = timedelta(hours=1)

    def __call__(self) -> timedelta:
        return self.now

    def advance(self, delta: timedelta) -> None:
        self.now += delta


class FailingReleasePersistence(InMemoryContextStorePersistence):
    def __init__(self) -> None:
        super().__init__()
        self.failing: set[ContextId] = set()

    @override
    def release_context(self, ctx: ContextId) -> None:
        if ctx in self.failing:
            self.failing.discard(ctx)
            raise RuntimeError(f"release of {ctx} failed")
        super().release_context(ctx)


class KeepingPersistence(InMemoryContextStorePersistence):
    """Behaves like a persistence that does not override release_context."""

    @override
    def release_context(self, ctx: ContextId) -> None:
        pass


def _store(
    persistence: InMemoryContextStorePersistence | None = None,
) -> tuple[ContextStore, InMemoryContextStorePersistence, FakeClock]:
    persistence = persistence or InMemoryContextStorePersistence()
    clock = FakeClock()
    return ContextStore.recover_from(persistence, clock=clock).context_store, persistence, clock


def _new_context(store: ContextStore, parents: tuple[ContextId, ...] = ()) -> ContextId:
    with store.create_context(parents=parents) as ctx:
        return ctx.id


def _exists(store: ContextStore, ctx: ContextId) -> bool:
    try:
        with store.get_context(ctx):
            return True
    except InvalidContextIdException:
        return False


def _persisted(persistence: InMemoryContextStorePersistence) -> set[ContextId]:
    return {entry.ctx for entry in persistence.log_entries()}


def test_idle_contexts_are_removed_from_store_and_persistence():
    store, persistence, clock = _store()
    parent = _new_context(store)
    child = _new_context(store, parents=(parent,))

    clock.advance(TTL + timedelta(seconds=1))

    assert store.expire_idle(TTL) == 2
    assert not _exists(store, parent)
    assert not _exists(store, child)
    assert _persisted(persistence) == set()
    with pytest.raises(InvalidContextIdException):
        _new_context(store, parents=(parent,))


def test_contexts_within_ttl_are_kept():
    store, _, clock = _store()
    ctx = _new_context(store)

    clock.advance(TTL)

    assert store.expire_idle(TTL) == 0
    assert _exists(store, ctx)


def test_expired_context_is_completed_before_release():
    persistence = KeepingPersistence()
    store, _, clock = _store(persistence)
    ctx = _new_context(store)
    clock.advance(TTL * 2)

    assert store.expire_idle(TTL) == 1

    assert isinstance(next(e for e in reversed(persistence.log_entries()) if e.ctx == ctx).data, ContextCompleted)
    recovered = ContextStore.recover_from(persistence).context_store
    assert not _exists(recovered, ctx)


def test_activity_resets_idle_timer():
    store, _, clock = _store()
    ctx = _new_context(store)

    clock.advance(TTL * 0.8)
    with store.get_context(ctx) as context:
        context.set_user_data("key", "value")
    clock.advance(TTL * 0.8)
    assert store.expire_idle(TTL) == 0

    _new_context(store, parents=(ctx,))
    clock.advance(TTL * 0.8)
    assert store.expire_idle(TTL) == 0
    assert _exists(store, ctx)

    clock.advance(TTL * 0.8)
    assert store.expire_idle(TTL) == 2


def test_recovered_contexts_are_active_at_recovery():
    persistence = InMemoryContextStorePersistence()
    original = ContextStore.recover_from(persistence).context_store
    ctx = _new_context(original)

    store, _, clock = _store(persistence)
    clock.advance(TTL * 0.8)
    assert store.expire_idle(TTL) == 0
    clock.advance(TTL * 0.8)
    assert store.expire_idle(TTL) == 1
    assert not _exists(store, ctx)


def test_child_outlives_expired_parent():
    store, _, clock = _store()
    parent = _new_context(store)
    child = _new_context(store, parents=(parent,))

    clock.advance(TTL * 0.8)
    with store.get_context(child) as context:
        context.append_user_note("still working")
    clock.advance(TTL * 0.8)

    assert store.expire_idle(TTL) == 1
    assert not _exists(store, parent)
    with store.get_context(child) as context:
        assert set(context.copy_parent_context_snapshots()) == {parent}
    _new_context(store, parents=(child,))


def test_locked_context_is_skipped_without_error_log(caplog: pytest.LogCaptureFixture):
    store, _, clock = _store()
    ctx = _new_context(store)
    clock.advance(TTL * 2)
    locked = threading.Event()
    release = threading.Event()

    def hold_context() -> None:
        with store.get_context(ctx):
            locked.set()
            release.wait()

    holder = threading.Thread(target=hold_context)
    holder.start()
    locked.wait()
    with caplog.at_level(logging.ERROR):
        assert store.expire_idle(TTL) == 0
    release.set()
    holder.join()

    assert not [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert store.expire_idle(TTL) == 1


def test_release_failure_does_not_block_other_contexts(caplog: pytest.LogCaptureFixture):
    persistence = FailingReleasePersistence()
    store, _, clock = _store(persistence)
    failing = _new_context(store)
    other = _new_context(store)
    persistence.failing.add(failing)
    clock.advance(TTL * 2)

    with caplog.at_level(logging.ERROR):
        assert store.expire_idle(TTL) == 1

    assert any(failing in record.getMessage() for record in caplog.records)
    assert not _exists(store, other)
    with store.get_context(failing) as context:
        assert context.is_completed

    assert store.expire_idle(TTL) == 1
    assert not _exists(store, failing)
    assert _persisted(persistence) == set()


def _collector_bus(
    store: ContextStore, context_ttl: timedelta | None
) -> tuple[EventBus, CollectorActor[str], Source[str], PipeToBus]:
    pipe_to_bus: PipeToBus = queue.Queue()
    source = Source[str]("expiry-source")
    collector = CollectorActor[str](pipe_to_bus=pipe_to_bus, context_store=store)
    piping = Piping()
    piping.add_flow(Flow.from_connectable(source).then(collector.sink))
    bus = EventBus(
        connections=piping.pipes,
        input_pipe=pipe_to_bus,
        actors=[collector],
        context_store=store,
        context_ttl=context_ttl,
    )
    return bus, collector, source, pipe_to_bus


def test_bus_without_ttl_never_expires_contexts():
    store, _, clock = _store()
    bus, collector, source, pipe_to_bus = _collector_bus(store, context_ttl=None)
    idle = _new_context(store)
    clock.advance(timedelta(days=1000))
    active = _new_context(store)

    pipe_to_bus.put(SendEvent(ctx_id=active, source=source, payload="hello"))
    pipe_to_bus.put(StopBusEvent())
    Jobs(bus.run_loop(), collector.run_loop()).join()

    assert [event.ctx_id for event in collector.received_events] == [active]
    assert _exists(store, idle)


def test_bus_drops_message_for_expired_context_and_keeps_running(caplog: pytest.LogCaptureFixture):
    store, _, clock = _store()
    bus, collector, source, pipe_to_bus = _collector_bus(store, context_ttl=None)
    expired = _new_context(store)
    clock.advance(TTL * 2)
    store.expire_idle(TTL)
    live = _new_context(store)

    pipe_to_bus.put(SendEvent(ctx_id=expired, source=source, payload="late"))
    pipe_to_bus.put(SendEvent(ctx_id=live, source=source, payload="on time"))
    with caplog.at_level(logging.ERROR):
        jobs = Jobs(bus.run_loop(), collector.run_loop())
        wait_until(lambda: len(collector.received_events) == 1)
        bus.request_stop()
        jobs.join()

    assert collector.received_events[0].ctx_id == live
    dropped = [record.getMessage() for record in caplog.records if record.levelno == logging.ERROR]
    assert len(dropped) == 1
    assert expired in dropped[0]
    assert source.id in dropped[0]


def test_bus_drops_message_for_context_whose_release_failed(caplog: pytest.LogCaptureFixture):
    persistence = FailingReleasePersistence()
    store, _, clock = _store(persistence)
    bus, collector, source, pipe_to_bus = _collector_bus(store, context_ttl=None)
    completed = _new_context(store)
    persistence.failing.add(completed)
    clock.advance(TTL * 2)
    store.expire_idle(TTL)
    live = _new_context(store)

    pipe_to_bus.put(SendEvent(ctx_id=completed, source=source, payload="late"))
    pipe_to_bus.put(SendEvent(ctx_id=live, source=source, payload="on time"))
    with caplog.at_level(logging.ERROR):
        jobs = Jobs(bus.run_loop(), collector.run_loop())
        wait_until(lambda: len(collector.received_events) == 1)
        bus.request_stop()
        jobs.join()

    assert collector.received_events[0].ctx_id == live
    assert any(
        "Dropping message" in record.getMessage() and completed in record.getMessage() for record in caplog.records
    )


def test_bus_sweeps_idle_contexts_while_no_events_arrive():
    store, _, clock = _store()
    ttl = timedelta(milliseconds=100)
    bus, collector, _, _ = _collector_bus(store, context_ttl=ttl)
    ctx = _new_context(store)

    jobs = Jobs(bus.run_loop(), collector.run_loop())
    clock.advance(ttl * 2)
    wait_until(lambda: not _exists(store, ctx))
    bus.request_stop()
    jobs.join()


@pytest.mark.parametrize("ttl", [timedelta(0), timedelta(seconds=-1)])
def test_subnet_builder_rejects_non_positive_ttl(ttl: timedelta):
    with pytest.raises(ValueError, match="context_ttl"):
        SubnetBuilder(nodes=[], context_ttl=ttl)


def test_subnet_builder_passes_ttl_to_event_bus():
    source = Source[str]("builder-source")
    store = ContextStore.recover_from(InMemoryContextStorePersistence()).context_store
    collector = CollectorActor[str](pipe_to_bus=queue.Queue(), context_store=store)
    runtime = (
        SubnetBuilder(nodes=[], context_store=store, context_ttl=TTL)
        .add_actors(collector)
        .add_flows(Flow.from_connectable(source).then(collector.sink))
        .build()
    )

    assert runtime.event_bus.context_ttl == TTL
