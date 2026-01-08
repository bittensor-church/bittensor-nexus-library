# pyright: basic

import pytest

from nexus.core.dsl.flow import Flow
from nexus.core.dsl.nodes import (
    DoubleTransform,
    Fork,
    Node,
    NodeSinks,
    NodeSources,
    Sink,
    SinkName,
    Source,
    SourceName,
    Transform,
)


class DualSinkPreferred(Node):
    def __init__(self) -> None:
        self.primary = Sink("primary")
        self.secondary = Sink("secondary")

    def sinks(self) -> NodeSinks:
        sinks = NodeSinks(
            sinks={
                SinkName("primary"): self.primary,
                SinkName("secondary"): self.secondary,
            }
        )
        sinks.default_sink = self.secondary
        return sinks

    def sources(self) -> NodeSources:
        return NodeSources(sources={})


def test_single_source_connects_to_single_sink() -> None:
    start = Source("start")
    end = Sink("end")

    flow = Flow.from_node(start).then(end)

    assert flow.pipes[start] == {end}
    assert flow.exit_sources.sources == {}


def test_single_source_connects_to_multiple_sinks() -> None:
    start = Source("start")
    a = Sink("a")
    b = Sink("b")

    flow = Flow.from_node(start).then(a, b)

    assert flow.pipes[start] == {a, b}
    assert flow.exit_sources.sources == {}


def test_positional_then_uses_default_exit_source_when_available() -> None:
    transform = Transform[str, str]("transform")
    ok_sink = Sink("ok-sink")

    flow = Flow.from_node(transform).then(ok_sink)

    assert flow.pipes[transform.ok] == {ok_sink}
    assert transform.error not in flow.pipes


def test_positional_then_uses_preferred_entry_sink_when_available() -> None:
    start = Source("start")
    preferred = DualSinkPreferred()

    flow = Flow.from_node(start).then(preferred)

    assert flow.pipes[start] == {preferred.secondary}
    assert preferred.primary not in flow.pipes[start]


def test_fork_routes_support_single_target_per_branch() -> None:
    start = Source("start")
    fork = Fork[str, str, str]("fork")
    left_sink = Sink("left")
    right_sink = Sink("right")

    flow = Flow.from_node(start).then(fork).then(left=left_sink, right=right_sink)

    assert flow.pipes[start] == {fork.sink}
    assert flow.pipes[fork.left] == {left_sink}
    assert flow.pipes[fork.right] == {right_sink}
    assert flow.exit_sources.sources == {}


def test_fork_routes_support_list_and_tuple_targets() -> None:
    fork = Fork[str, str, str]("fork")
    left_a = Sink("left-a")
    left_b = Sink("left-b")
    right_a = Sink("right-a")
    right_b = Sink("right-b")

    flow = Flow.from_node(fork).then(left=[left_a, left_b], right=(right_a, right_b))

    assert flow.pipes[fork.left] == {left_a, left_b}
    assert flow.pipes[fork.right] == {right_a, right_b}
    assert flow.exit_sources.sources == {}


def test_fork_routes_support_flow_targets_and_absorb_internal_pipes() -> None:
    start = Source("start")
    fork = Fork[str, str, str]("fork")

    left_transform = Transform[str, str]("left-transform")
    left_ok = Sink("left-ok")
    left_error = Sink("left-error")
    left_flow = Flow.from_node(left_transform).then(ok=left_ok, error=left_error)

    right_sink = Sink("right")

    flow = Flow.from_node(start).then(fork).then(left=left_flow, right=right_sink)

    assert flow.pipes[start] == {fork.sink}
    assert flow.pipes[fork.left] == {left_transform.sink}
    assert flow.pipes[left_transform.ok] == {left_ok}
    assert flow.pipes[left_transform.error] == {left_error}
    assert flow.pipes[fork.right] == {right_sink}
    assert flow.exit_sources.sources == {}


def test_error_when_source_cannot_be_implied() -> None:
    fork = Fork[str, str, str]("fork")
    target = Sink("target")

    with pytest.raises(AssertionError, match="No default exit source"):
        Flow.from_node(fork).then(target)


def test_error_when_sink_cannot_be_implied() -> None:
    start = Source("start")
    ambiguous = DoubleTransform[str, str, str, str]("double")

    with pytest.raises(AssertionError, match="No default entry sink"):
        Flow.from_node(start).then(ambiguous)


def test_error_if_fork_branch_is_misnamed() -> None:
    fork = Fork[str, str, str]("fork")

    with pytest.raises(AssertionError, match="Unexpected connection"):
        Flow.from_node(fork).then(ok=Sink("sink"))


def test_error_if_keyword_route_uses_unknown_source_name() -> None:
    start = Source("start")

    with pytest.raises(AssertionError, match="Unexpected connection"):
        Flow.from_node(start).then(ok=Sink("sink"))


def test_error_when_keyword_routes_used_after_flow_has_no_exit_sources() -> None:
    start = Source("start")
    end = Sink("end")
    flow = Flow.from_node(start).then(end)

    with pytest.raises(AssertionError, match="Unexpected connection"):
        flow.then(ok=Sink("another"))


def test_positional_then_continues_from_single_flow_target_with_sources() -> None:
    start = Source("start")
    side_effect = Sink("side-effect")

    transform = Transform[str, str]("t")
    subflow = Flow.from_node(transform)

    flow = Flow.from_node(start).then(side_effect, subflow)

    assert flow.pipes[start] == {side_effect, transform.sink}
    assert flow.exit_sources.sources[SourceName("ok")] is transform.ok
    assert flow.exit_sources.sources[SourceName("error")] is transform.error


def test_self_loops_are_allowed() -> None:
    start = Source("start")
    transform = Transform[str, str]("t")

    flow = Flow.from_node(start).then(transform).then(transform)

    assert flow.pipes[start] == {transform.sink}
    assert flow.pipes[transform.ok] == {transform.sink}


def test_routes_to_flow_without_entry_sink_should_error_cleanly() -> None:
    start = Source("start")
    subflow = Flow.from_node(Source("sub-start"))  # no entry sink

    with pytest.raises(AssertionError, match="No default entry sink"):
        Flow.from_node(start).then(subflow)
