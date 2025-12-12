# pyright: basic

import logging
import queue
from typing import Any, override

from utils import Jobs, wait_until

from nexus.context_store import ContextId
from nexus.piping.dsl import Piping, Sink, Source, SourceId, Transform
from nexus.runtime.actor import Actor, EventHandler, TransformActor
from nexus.runtime.event_bus import EventBus
from nexus.runtime.events import Event, PipeToBus, ReceiveEvent, SendEvent, StopActorEvent, StopBusEvent
from nexus.transforms.stringify import Stringify, StringifyActor


class DualSinkActor(Actor):
    def __init__(self, name: str = "dual-sink") -> None:
        super().__init__(name=name, pipe_to_bus=queue.Queue())
        self.sink_left = Sink("left")
        self.sink_right = Sink("right")
        self.handled_left: list[Event] = []
        self.handled_right: list[Event] = []

    def handlers(self) -> dict[Sink, EventHandler]:
        return {
            self.sink_left: self.handle_left,
            self.sink_right: self.handle_right,
        }

    def handle_left(self, receive_event: ReceiveEvent) -> None:
        self.handled_left.append(receive_event.payload)

    def handle_right(self, receive_event: ReceiveEvent) -> None:
        self.handled_right.append(receive_event.payload)


class CollectorActor(Actor):
    def __init__(self, *, pipe_to_bus: PipeToBus, name="collector"):
        super().__init__(name=name, pipe_to_bus=pipe_to_bus)
        self.sink = Sink(name)
        self.received_events = []

    def handlers(self):
        return {self.sink: self._handle}

    def _handle(self, event: ReceiveEvent) -> None:
        self.received_events.append(event)


class FaultyTransformActor(TransformActor):
    def __init__(self, *, name="faulty", pipe_to_bus) -> None:
        super().__init__(spec=Transform(name), pipe_to_bus=pipe_to_bus)

    @override
    def _transform(self, ctx: ContextId, payload: Any):  # pragma: no cover - exercised in tests
        raise ValueError("boom")


class FlakyActor(Actor):
    """Actor that raises on specific payloads but forwards others."""

    def __init__(self, *, pipe_to_bus: PipeToBus) -> None:
        super().__init__(name="flaky", pipe_to_bus=pipe_to_bus)
        self.sink = Sink("flaky-sink")
        self.source = Source("flaky-source")

    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {
            self.sink: self.handle
        }

    def handle(self, event: ReceiveEvent[Any]) -> None:
        if event.payload == "boom":
            raise ValueError("flaky failure")
        else:
            self.pipe_to_bus.put(SendEvent(ctx=event.ctx, source=self.source, payload=f"ok-{event.payload}"))


def test_actor_dispatches_events_to_handlers():
    actor = DualSinkActor()

    events = [
        ReceiveEvent(ctx=ContextId("ctx-left-1"), target=actor.sink_left, payload="payload-left-1"),
        ReceiveEvent(ctx=ContextId("ctx-right"), target=actor.sink_right, payload="payload-right"),
        ReceiveEvent(ctx=ContextId("ctx-left-2"), target=actor.sink_left, payload="payload-left-2"),
        StopActorEvent(),
    ]

    for event in events:
        actor.pipe_from_bus.put(event)
    actor_job = actor.run_loop()

    actor_job.join(1.0)
    assert not actor_job.is_alive()

    assert actor.handled_left == ["payload-left-1", "payload-left-2"]
    assert actor.handled_right == ["payload-right"]



def test_transform_actor_emits_transformed_event():
    stringify = Stringify()  # stringify is a test transform anyway, so let's use it here
    pipe_to_bus = queue.Queue()
    pipe_from_bus = queue.Queue()
    actor = StringifyActor(spec=stringify, pipe_to_bus=pipe_to_bus)

    context = ContextId("ctx-123")

    actor.pipe_from_bus.put(
        ReceiveEvent(ctx=context, target=stringify.sink, payload=123)
    )
    actor.pipe_from_bus.put(StopActorEvent())

    actor_job = actor.run_loop()
    actor_job.join(1.0)
    assert not actor_job.is_alive()

    send_event = pipe_to_bus.get_nowait()
    assert send_event.payload == "123"
    assert send_event.source == stringify.ok
    assert send_event.ctx == context
    assert pipe_to_bus.empty()


def test_context_id_preserved_through_transform_and_bus():
    stringify = Stringify()

    pipe_to_bus = queue.Queue()

    collector_actor = CollectorActor(pipe_to_bus=pipe_to_bus)
    transform_actor = StringifyActor(spec=stringify, pipe_to_bus=pipe_to_bus)

    piping = Piping()
    piping.connect(stringify.ok, collector_actor.sink)
    event_bus = EventBus(
        connections=piping.pipes,
        input_pipe=pipe_to_bus,
        actors=[transform_actor, collector_actor],
    )

    context = ContextId("ctx-pass-through")
    transform_actor.pipe_from_bus.put(
        ReceiveEvent(ctx=context, target=stringify.sink, payload=42)
    )
    jobs = Jobs(
        transform_actor.run_loop(),
        collector_actor.run_loop(),
        event_bus.run_loop())

    wait_until(lambda: len(collector_actor.received_events) == 1)
    handled_event = collector_actor.received_events[0]
    assert handled_event.ctx == context
    assert handled_event.payload == "42"

    event_bus.request_stop()
    jobs.join()

def test_event_bus_routes_events_to_configured_sinks():
    pipe_to_bus = queue.Queue()

    broadcast = Source("broadcast")
    collector_a = CollectorActor(name="collector-a", pipe_to_bus=pipe_to_bus)
    collector_b = CollectorActor(name="collector-b", pipe_to_bus=pipe_to_bus)

    piping = Piping()
    piping.connect(broadcast, collector_a.sink)
    piping.connect(broadcast, collector_b.sink)

    event_bus = EventBus(
        connections=piping.pipes,
        input_pipe=pipe_to_bus,
        actors=[collector_a, collector_b],
    )

    ctx = ContextId("fan-out")
    pipe_to_bus.put(SendEvent(ctx=ctx, source=broadcast, payload="hello"))
    pipe_to_bus.put(StopBusEvent())

    jobs = Jobs(
        event_bus.run_loop(),
        collector_a.run_loop(),
        collector_b.run_loop())

    wait_until(lambda: len(collector_a.received_events) == 1)
    wait_until(lambda: len(collector_b.received_events) == 1)

    assert [event.payload for event in collector_a.received_events] == ["hello"]
    assert [event.payload for event in collector_b.received_events] == ["hello"]

    event_bus.request_stop()
    jobs.join()


def test_event_bus_logs_when_no_connections(caplog: Any):
    pipe_to_bus = queue.Queue()

    event_bus = EventBus(connections=Piping().pipes, input_pipe=pipe_to_bus, actors=[])
    source = Source("orphan")
    ctx = ContextId("ctx-none")

    with caplog.at_level(logging.ERROR):
        pipe_to_bus.put(SendEvent(ctx=ctx, source=source, payload="payload"))
        event_loop = event_bus.run_loop()

        wait_until(lambda: any("No connections found" in record.message for record in caplog.records))

    event_bus.request_stop()
    event_loop.join(1.0)
    assert not event_loop.is_alive()


def test_actor_error_does_not_stop_event_bus(caplog: Any):
    pipe_to_bus = queue.Queue()
    source = Source("source")
    flaky = FlakyActor(pipe_to_bus=pipe_to_bus)

    collector = CollectorActor(pipe_to_bus=pipe_to_bus)

    piping = Piping()
    piping.connect(source, flaky.sink)
    piping.connect(flaky.source, collector.sink)

    event_bus = EventBus(
        connections=piping.pipes,
        input_pipe=pipe_to_bus,
        actors=[flaky, collector])

    pipe_to_bus.put(SendEvent(ctx=ContextId("ctx-flaky-1"), source=source, payload="good-1"))
    pipe_to_bus.put(SendEvent(ctx=ContextId("ctx-flaky-2"), source=source, payload="boom"))
    pipe_to_bus.put(SendEvent(ctx=ContextId("ctx-flaky-3"), source=source, payload="good-2"))

    with caplog.at_level(logging.ERROR):
        jobs = Jobs(event_bus.run_loop(), flaky.run_loop(), collector.run_loop())
        wait_until(lambda: len(collector.received_events) == 2)

    event_bus.request_stop()
    jobs.join()

    assert [(event.payload, event.ctx.id) for event in collector.received_events] == [
        ("ok-good-1", "ctx-flaky-1"),
        ("ok-good-2", "ctx-flaky-3"),
    ]
    error_records = [
        record for record in caplog.records if "Error while handling event" in record.message
    ]
    assert len(error_records) == 1
    assert any(
        record.exc_info and isinstance(record.exc_info[1], ValueError) and str(record.exc_info[1]) == "flaky failure"
        for record in error_records
    )
