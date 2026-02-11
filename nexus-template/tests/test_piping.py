# pyright: basic

from nexus.core.dsl.flow import Flow
from nexus.core.dsl.nodes import Sink, Source
from nexus.core.dsl.piping import Piping


def test_add_flow_aggregates_sources_sinks_and_pipes_from_multiple_flows() -> None:
    scattering_source = Source("scattering-source")
    sink_a = Sink("sink-a")
    sink_b = Sink("sink-b")
    other_source = Source("other-source")
    aggregating_sink = Sink("aggregating-sink")

    flow_one = Flow.from_connectable(scattering_source).then(sink_a)
    flow_two = Flow.from_connectable(scattering_source).then(sink_b)
    flow_three = Flow.from_connectable(other_source).then(aggregating_sink)

    piping = Piping()
    piping.add_flow(flow_one)
    piping.add_flow(flow_two)
    piping.add_flow(flow_three)

    assert piping.sources == {scattering_source, other_source}
    assert piping.sinks == {sink_a, sink_b, aggregating_sink}
    assert piping.pipes[scattering_source] == {sink_a, sink_b}
    assert piping.pipes[other_source] == {aggregating_sink}
