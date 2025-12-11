# pyright: basic

import logging
import queue
from typing import Any, Literal

from nexus.context_store import ContextId
from nexus.piping.dsl import Piping, Sink, SinkId, Source, SourceId, Transform
from nexus.runtime.actor import Actor, EventHandler, TransformActor
from nexus.runtime.event_bus import EventBus
from nexus.runtime.events import FromBus, ReceiveEvent, SendEvent, StopActorEvent, StopBusEvent, ToBus
from nexus.transforms.stringify import Stringify, StringifyActor


class DualSinkActor(Actor):
    def __init__(self, sink_left: Sink, sink_right: Sink) -> None:
        super().__init__(name="dual-sink", to_bus=queue.Queue())
        self.sink_left = sink_left
        self.sink_right = sink_right
        self.handled: list[tuple[Literal["left-sink", "right-sink"], str]] = []
        self.from_bus: FromBus = queue.Queue[ReceiveEvent[Any]]()

    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {
            self.sink_left: self.handle_left,
            self.sink_right: self.handle_right,
        }

    def handle_left(self, event: ReceiveEvent[str]) -> None:
        self.handled.append(("left-sink", event.payload))

    def handle_right(self, event: ReceiveEvent[str]) -> None:
        self.handled.append(("right-sink", event.payload))


class CollectingActor(Actor):
    def __init__(self, sink: Sink, name: str = "collector") -> None:
        super().__init__(name=name, to_bus=queue.Queue())
        self.sink = sink
        self.received_events = []
        self.from_bus = queue.Queue()

    def handlers(self):
        return {self.sink: self._handle}

    def _handle(self, event: ReceiveEvent) -> None:
        self.received_events.append(event)


class FaultyTransformActor(TransformActor):
    def __init__(self, *, to_bus: ToBus) -> None:
        self.spec = Transform("faulty")
        super().__init__(name="faulty", spec=self.spec, to_bus=to_bus)
        self.from_bus: FromBus = queue.Queue()

    def _transform(self, ctx: ContextId, payload: int) -> int:  # pragma: no cover - exercised in tests
        raise ValueError("boom")


class FlakyActor(Actor):
    """Actor that raises on specific payloads but forwards others."""

    def __init__(self, sink: Sink, source: Source, to_bus: ToBus) -> None:
        super().__init__(name="flaky", to_bus=to_bus)
        self.sink = sink
        self.source = source
        self.from_bus: FromBus = queue.Queue()

    def handlers(self):
        return {self.sink: self._handle}

    def _handle(self, event: ReceiveEvent[str]) -> None:
        if event.payload == "boom":
            raise ValueError("flaky failure")
        self.to_bus.put(SendEvent(ctx=event.ctx, source=self.source, payload=f"ok-{event.payload}"))


def test_actor_dispatches_events_to_handlers():
    sink_left: Sink[str] = Sink(SinkId("left"))
    sink_right: Sink[str] = Sink(SinkId("right"))
    actor = DualSinkActor(sink_left, sink_right)

    actor.from_bus.put(
        ReceiveEvent(ctx=ContextId("ctx-1"), target=sink_left, payload="payload-one")
    )
    actor.from_bus.put(
        ReceiveEvent(ctx=ContextId("ctx-2"), target=sink_right, payload="payload-two")
    )
    actor.from_bus.put(StopActorEvent())

    actor.loop()

    assert actor.handled == [
        ("left-sink", "payload-one"),
        ("right-sink", "payload-two"),
    ]


def test_transform_actor_emits_transformed_event():
    stringify = Stringify()
    to_bus = queue.Queue()
    actor = StringifyActor(spec=stringify, to_bus=to_bus)
    actor.from_bus = queue.Queue()
    context = ContextId("ctx-123")

    actor.from_bus.put(
        ReceiveEvent(ctx=context, target=stringify.sink, payload=123)
    )
    actor.from_bus.put(StopActorEvent())

    actor.loop()

    send_event = to_bus.get_nowait()
    assert send_event.payload == "123"
    assert send_event.source == stringify.source
    assert send_event.ctx == context
    assert to_bus.empty()


def test_context_id_preserved_through_transform_and_bus():
    stringify = Stringify()
    to_bus = queue.Queue()
    transform_actor = StringifyActor(spec=stringify, to_bus=to_bus)
    transform_actor.from_bus = queue.Queue[ReceiveEvent[Any]]()

    collector_sink: Sink[str] = Sink(SinkId("collector"))
    collector_actor = CollectingActor(collector_sink)

    piping = Piping()
    piping.connect(stringify.source, collector_sink)
    event_bus = EventBus(
        connections=piping.pipes,
        input_pipe=to_bus,
        sinks={collector_sink: collector_actor},
    )

    context = ContextId("ctx-pass-through")
    transform_actor.from_bus.put(
        ReceiveEvent(ctx=context, target=stringify.sink, payload=42)
    )
    transform_actor.from_bus.put(StopActorEvent())
    transform_actor.loop()

    to_bus.put(StopBusEvent())
    event_bus.loop()

    collector_actor.loop()

    assert len(collector_actor.received_events) == 1
    handled_event = collector_actor.received_events[0]
    assert handled_event.ctx == context
    assert handled_event.payload == "42"


def test_event_bus_routes_events_to_configured_sinks():
    source: Source[str] = Source(SourceId("broadcast"))
    sink_a: Sink[str] = Sink(SinkId("sink-a"))
    sink_b: Sink[str] = Sink(SinkId("sink-b"))
    actor_a = CollectingActor(sink_a, name="collector-a")
    actor_b = CollectingActor(sink_b, name="collector-b")

    piping = Piping()
    piping.connect(source, sink_a)
    piping.connect(source, sink_b)

    input_pipe = queue.Queue()
    event_bus = EventBus(
        connections=piping.pipes,
        input_pipe=input_pipe,
        sinks={sink_a: actor_a, sink_b: actor_b},
    )

    ctx = ContextId("fan-out")
    input_pipe.put(SendEvent(ctx=ctx, source=source, payload="hello"))
    input_pipe.put(StopBusEvent())

    event_bus.loop()

    actor_a.loop()
    actor_b.loop()

    assert [event.payload for event in actor_a.received_events] == ["hello"]
    assert [event.payload for event in actor_b.received_events] == ["hello"]


def test_event_bus_logs_when_no_connections(caplog: Any):
    input_pipe: queue.Queue[SendEvent[Any]] = queue.Queue()
    event_bus = EventBus(connections=Piping().pipes, input_pipe=input_pipe, sinks={})
    source: Source[str] = Source(SourceId("orphan"))
    ctx = ContextId("ctx-none")

    with caplog.at_level(logging.ERROR):
        input_pipe.put(SendEvent(ctx=ctx, source=source, payload="payload"))
        input_pipe.put(StopBusEvent())
        event_bus.loop()

    assert any("No connections found" in record.message for record in caplog.records)


def test_actor_error_does_not_stop_event_bus(caplog: Any):
    source = Source(SourceId("source"))
    flaky_sink = Sink(SinkId("flaky-sink"))
    flaky_output = Source(SourceId("flaky-output"))
    to_bus = queue.Queue()
    actor = FlakyActor(sink=flaky_sink, source=flaky_output, to_bus=to_bus)

    piping = Piping()
    piping.connect(source, flaky_sink)

    input_pipe = queue.Queue()
    event_bus = EventBus(connections=piping.pipes, input_pipe=input_pipe, sinks={flaky_sink: actor})

    with caplog.at_level(logging.ERROR):
        input_pipe.put(SendEvent(ctx=ContextId("ctx-flaky-1"), source=source, payload="good-1"))
        input_pipe.put(SendEvent(ctx=ContextId("ctx-flaky-2"), source=source, payload="boom"))
        input_pipe.put(SendEvent(ctx=ContextId("ctx-flaky-3"), source=source, payload="good-2"))
        input_pipe.put(StopBusEvent())
        event_bus.loop()
        actor.loop()

    processed_events: list[SendEvent[str]] = []
    while not to_bus.empty():
        processed_events.append(to_bus.get_nowait())

    assert [(event.payload, event.ctx.id) for event in processed_events] == [
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
