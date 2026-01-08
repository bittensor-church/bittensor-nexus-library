# pyright: basic

import logging
import queue
import unittest
from typing import Any, override

from utils import Jobs, wait_until

from nexus.actors import (
    EvenSucks,
    Stringify,
    StringifyActor,
    UppercaseOrError,
    UppercaseOrErrorActor,
)
from nexus.context_store import ContextId
from nexus.core.dsl.flow import Flow
from nexus.core.dsl.nodes import DoubleTransform, Fork, Sink, Source, SourceName, Transform
from nexus.core.dsl.piping import Piping
from nexus.core.runtime.actor import Actor, EventHandler
from nexus.core.runtime.actor_patterns import DoubleTransformActor, ForkActor, TransformActor
from nexus.core.runtime.event_bus import EventBus
from nexus.core.runtime.events import Event, PipeToBus, ReceiveEvent, SendEvent, StopActorEvent, StopBusEvent


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
    def _transform(self, ctx: ContextId, payload: Any):
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


class BranchingForkActor(ForkActor[str, str, str]):
    def __init__(self, *, pipe_to_bus: PipeToBus) -> None:
        super().__init__(spec=Fork[str, str, str](gid_prefix="branching-fork"), pipe_to_bus=pipe_to_bus)

    @override
    def _process(self, ctx: ContextId, payload: str) -> tuple[str, None] | tuple[None, str]:
        if payload.startswith("left"):
            return payload, None
        else:
            return None, payload


class TestDoubleTransformActor(DoubleTransformActor[str, str, str, str]):
    def __init__(self, *, pipe_to_bus: PipeToBus) -> None:
        spec = DoubleTransform[str, str, str, str](gid_prefix="instrumented-double")
        super().__init__(
            name=spec.gid,
            input_spec=spec.input_transform,
            output_spec=spec.output_transform,
            pipe_to_bus=pipe_to_bus,
        )

    @override
    def _transform_input(self, ctx: ContextId, payload: str) -> str:
        if payload.startswith("fail"):
            raise ValueError("input-failed")
        return payload.upper()

    @override
    def _transform_output(self, ctx: ContextId, payload: str) -> str:
        if payload.startswith("fail"):
            raise ValueError("output-failed")
        return f"{payload}-out"


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


def test_fork_actor_routes_left_and_right_sources():
    pipe_to_bus = queue.Queue()
    actor = BranchingForkActor(pipe_to_bus=pipe_to_bus)

    ctx_left = ContextId("ctx-left")
    ctx_right = ContextId("ctx-right")

    actor.pipe_from_bus.put(ReceiveEvent(ctx=ctx_left, target=actor.spec.sink, payload="left-payload"))
    actor.pipe_from_bus.put(ReceiveEvent(ctx=ctx_right, target=actor.spec.sink, payload="right-payload"))
    actor.pipe_from_bus.put(StopActorEvent())

    actor_job = actor.run_loop()
    actor_job.join(1.0)
    assert not actor_job.is_alive()

    left_event = pipe_to_bus.get_nowait()
    right_event = pipe_to_bus.get_nowait()

    assert left_event.source == actor.spec.left
    assert left_event.payload == "left-payload"
    assert left_event.ctx == ctx_left

    assert right_event.source == actor.spec.right
    assert right_event.payload == "right-payload"
    assert right_event.ctx == ctx_right

    assert pipe_to_bus.empty()


def test_fork_actor_preserves_context_id():
    pipe_to_bus = queue.Queue()
    actor = BranchingForkActor(pipe_to_bus=pipe_to_bus)
    ctx = ContextId("ctx-fork-preserve")

    actor.pipe_from_bus.put(ReceiveEvent(ctx=ctx, target=actor.spec.sink, payload="left-ctx"))
    actor.pipe_from_bus.put(StopActorEvent())

    job = actor.run_loop()
    job.join(1.0)
    assert not job.is_alive()

    emitted = pipe_to_bus.get_nowait()
    assert emitted.ctx == ctx
    assert emitted.source == actor.spec.left
    assert emitted.payload == "left-ctx"
    assert pipe_to_bus.empty()


def test_transform_actor_emits_transformed_event():
    stringify = Stringify()  # stringify is a test transform anyway, so let's use it here
    pipe_to_bus = queue.Queue()
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


def test_transform_actor_routes_ok_and_error_sources():
    transform = UppercaseOrError()
    pipe_to_bus = queue.Queue()
    actor = UppercaseOrErrorActor(spec=transform, pipe_to_bus=pipe_to_bus)

    ctx_ok = ContextId("ctx-transform-ok")
    ctx_error = ContextId("ctx-transform-error")

    actor.pipe_from_bus.put(ReceiveEvent(ctx=ctx_ok, target=transform.sink, payload="odd"))
    actor.pipe_from_bus.put(ReceiveEvent(ctx=ctx_error, target=transform.sink, payload="boom"))
    actor.pipe_from_bus.put(StopActorEvent())

    job = actor.run_loop()
    job.join(1.0)
    assert not job.is_alive()

    events = [pipe_to_bus.get_nowait(), pipe_to_bus.get_nowait()]
    ok_event = next(event for event in events if event.ctx == ctx_ok)
    error_event = next(event for event in events if event.ctx == ctx_error)

    assert ok_event.source == transform.ok
    assert ok_event.payload == "ODD"
    assert ok_event.ctx == ctx_ok

    assert error_event.source == transform.error
    assert isinstance(error_event.payload, EvenSucks)
    assert error_event.ctx == ctx_error
    assert pipe_to_bus.empty()


def test_transform_actor_preserves_context_id():
    stringify = Stringify()
    pipe_to_bus = queue.Queue()
    actor = StringifyActor(spec=stringify, pipe_to_bus=pipe_to_bus)

    ctx = ContextId("ctx-transform-preserve")
    actor.pipe_from_bus.put(ReceiveEvent(ctx=ctx, target=stringify.sink, payload=7))
    actor.pipe_from_bus.put(StopActorEvent())

    job = actor.run_loop()
    job.join(1.0)
    assert not job.is_alive()

    sent = pipe_to_bus.get_nowait()
    assert sent.ctx == ctx
    assert sent.payload == "7"
    assert sent.source == stringify.ok
    assert pipe_to_bus.empty()


def test_event_bus_preserves_context_id():
    pipe_to_bus = queue.Queue()

    source = Source("context-source")
    collector = CollectorActor(pipe_to_bus=pipe_to_bus)

    piping = Piping()
    piping.connect(source, collector.sink)

    event_bus = EventBus(connections=piping.pipes, input_pipe=pipe_to_bus, actors=[collector])

    ctx = ContextId("ctx-bus-preserve")
    pipe_to_bus.put(SendEvent(ctx=ctx, source=source, payload="bus-payload"))

    jobs = Jobs(event_bus.run_loop(), collector.run_loop())
    wait_until(lambda: len(collector.received_events) == 1)

    received = collector.received_events[0]
    assert received.ctx == ctx
    assert received.payload == "bus-payload"

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


def test_double_transform_actor_routes_input_output_and_errors():
    pipe_to_bus = queue.Queue()
    actor = TestDoubleTransformActor(pipe_to_bus=pipe_to_bus)

    ctx_input_ok = ContextId("ctx-input-ok")
    ctx_input_error = ContextId("ctx-input-error")
    ctx_output_ok = ContextId("ctx-output-ok")
    ctx_output_error = ContextId("ctx-output-error")

    actor.pipe_from_bus.put(ReceiveEvent(ctx=ctx_input_ok, target=actor.input_spec.sink, payload="success"))
    actor.pipe_from_bus.put(ReceiveEvent(ctx=ctx_input_error, target=actor.input_spec.sink, payload="fail-input"))
    actor.pipe_from_bus.put(ReceiveEvent(ctx=ctx_output_ok, target=actor.output_spec.sink, payload="payload"))
    actor.pipe_from_bus.put(ReceiveEvent(ctx=ctx_output_error, target=actor.output_spec.sink, payload="fail-output"))
    actor.pipe_from_bus.put(StopActorEvent())

    actor_job = actor.run_loop()
    actor_job.join(1.0)
    assert not actor_job.is_alive()

    events = [pipe_to_bus.get_nowait() for _ in range(4)]

    def _event_by_ctx(ctx: ContextId) -> SendEvent[Any]:
        return next(event for event in events if event.ctx == ctx)

    input_ok = _event_by_ctx(ctx_input_ok)
    input_error = _event_by_ctx(ctx_input_error)
    output_ok = _event_by_ctx(ctx_output_ok)
    output_error = _event_by_ctx(ctx_output_error)

    assert input_ok.source == actor.input_spec.ok
    assert input_ok.payload == "SUCCESS"
    assert input_ok.ctx == ctx_input_ok

    assert input_error.source == actor.input_spec.error
    assert isinstance(input_error.payload, ValueError)
    assert str(input_error.payload) == "input-failed"
    assert input_error.ctx == ctx_input_error

    assert output_ok.source == actor.output_spec.ok
    assert output_ok.payload == "payload-out"
    assert output_ok.ctx == ctx_output_ok

    assert output_error.source == actor.output_spec.error
    assert isinstance(output_error.payload, ValueError)
    assert str(output_error.payload) == "output-failed"
    assert output_error.ctx == ctx_output_error

    assert pipe_to_bus.empty()


def test_double_transform_actor_preserves_context_id():
    pipe_to_bus = queue.Queue()
    actor = TestDoubleTransformActor(pipe_to_bus=pipe_to_bus)

    ctx_input = ContextId("ctx-double-input")
    ctx_output = ContextId("ctx-double-output")

    actor.pipe_from_bus.put(ReceiveEvent(ctx=ctx_input, target=actor.input_spec.sink, payload="in"))
    actor.pipe_from_bus.put(ReceiveEvent(ctx=ctx_output, target=actor.output_spec.sink, payload="out"))
    actor.pipe_from_bus.put(StopActorEvent())

    job = actor.run_loop()
    job.join(1.0)
    assert not job.is_alive()

    events = [pipe_to_bus.get_nowait(), pipe_to_bus.get_nowait()]
    input_event = next(event for event in events if event.ctx == ctx_input)
    output_event = next(event for event in events if event.ctx == ctx_output)

    assert input_event.ctx == ctx_input
    assert input_event.payload == "IN"
    assert input_event.source == actor.input_spec.ok

    assert output_event.ctx == ctx_output
    assert output_event.payload == "out-out"
    assert output_event.source == actor.output_spec.ok

    assert pipe_to_bus.empty()


class TestFlow(unittest.TestCase):
    def test_then_requires_targets(self) -> None:
        flow = Flow.from_node(Source("start"))
        with self.assertRaises(AssertionError):
            flow.then()

    def test_then_rejects_mixed_positional_and_keyword(self) -> None:
        flow = Flow.from_node(Source("start"))
        with self.assertRaises(AssertionError):
            flow.then(Sink("a"), ok=Sink("b"))

    def test_then_positional_connects_all_targets_and_continues_from_single_processing_target(self) -> None:
        start = Source("start")
        main = Transform[str, str]("main")
        side_effect = Sink("side-effect")
        end = Sink("end")

        flow = Flow.from_node(start).then(main, side_effect).then(ok=end)

        self.assertEqual(flow.pipes[start], {main.sink, side_effect})
        self.assertEqual(flow.pipes[main.ok], {end})
        self.assertNotIn(main.error, flow.pipes)

    def test_then_positional_continuation_target_independent_of_order(self) -> None:
        start = Source("start")
        main = Transform[str, str]("main")
        side_effect = Sink("side-effect")

        flow = Flow.from_node(start).then(side_effect, main)

        self.assertEqual(flow.pipes[start], {main.sink, side_effect})
        self.assertIs(flow.exit_sources.sources[SourceName("ok")], main.ok)
        self.assertIs(flow.exit_sources.sources[SourceName("error")], main.error)

    def test_then_positional_raises_when_multiple_targets_have_sources(self) -> None:
        start = Source("start")
        left = Transform[str, str]("left")
        right = Transform[str, str]("right")

        flow = Flow.from_node(start)
        with self.assertRaises(AssertionError):
            flow.then(left, right)

    def test_then_keyword_connects_named_sources_and_ends_flow(self) -> None:
        transform = Transform[str, str]("transform")
        ok_sink = Sink("ok-sink")
        error_sink = Sink("error-sink")

        flow = Flow.from_node(transform).then(ok=ok_sink, error=error_sink)

        self.assertEqual(flow.pipes[transform.ok], {ok_sink})
        self.assertEqual(flow.pipes[transform.error], {error_sink})
        self.assertEqual(flow.exit_sources.sources, {})

    def test_then_keyword_supports_multiple_targets_per_source(self) -> None:
        transform = Transform[str, str]("transform")
        ok_a = Sink("ok-a")
        ok_b = Sink("ok-b")
        error_sink = Sink("error-sink")

        flow = Flow.from_node(transform).then(ok=[ok_a, ok_b], error=error_sink)

        self.assertEqual(flow.pipes[transform.ok], {ok_a, ok_b})
        self.assertEqual(flow.pipes[transform.error], {error_sink})
        self.assertEqual(flow.exit_sources.sources, {})

    def test_then_keyword_raises_on_unknown_source_name(self) -> None:
        transform = Transform[str, str]("transform")
        flow = Flow.from_node(transform)
        with self.assertRaises(AssertionError):
            flow.then(does_not_exist=Sink("sink"))

    def test_then_merges_target_flow_pipes(self) -> None:
        start = Source("start")
        a = Transform[str, str]("a")
        b = Transform[str, str]("b")
        end = Sink("end")

        subflow = Flow.from_node(a).then(b)
        flow = Flow.from_node(start).then(subflow).then(ok=end)

        self.assertEqual(flow.pipes[start], {a.sink})
        self.assertEqual(flow.pipes[a.ok], {b.sink})
        self.assertEqual(flow.pipes[b.ok], {end})
