from __future__ import annotations

from collections.abc import Iterable
from typing import Any, NewType

from nexus.utils.exceptions import FlowMisconfiguredException

from .nodes import Node, NodeSinks, NodeSources, Pipes, Sink, SinkNode, Source, SourceName, SourceNode

SinkPath = NewType("SinkPath", str)
SourcePath = NewType("SourcePath", str)

"""Example flow use:

    entry: RestEntryPoint[SingleCatImageInput] = RestEntryPoint(
        path="/cat",
        port=8080,
        user_data_model=SingleCatImageInput)

    stringify: Stringify[SingleCatImageInput] = Stringify()

    upppercase_if_even: UppercaseIfEven = UppercaseIfEven()

    mining_task: UppercaseOrError = UppercaseOrError()
    printer: StringPrinter = StringPrinter()
    validation_task: UppercaseOrError = UppercaseOrError()

    validation_side_effect: StringPrinter = StringPrinter()
    validation_error_side_effect: StringPrinter = StringPrinter()

    bidi_transform: UppercaseInputLowercaseOutput = UppercaseInputLowercaseOutput()

    mining_flow: Flow = (
        # flow has some input (sink) and output (source)
        flow(bidi_transform.input_transform)
        # with "then" you can chain another sink; mining_task has only one sink,
        # so there is no need to specify exactly mining_task.sink
        .then(mining_task, printer)
        .then(
            # mining_task has two sources. one is named 'ok', and it should be connected to the output_transform sink
            ok=(
                flow(bidi_transform.output_transform)
                .then(...)
                .then(...)
                .then(merge_actor))
            # the other source is named 'error', and it should be connected to the input_transform sink; in this case
            # the input transform sink is a backreference to an earlier node in the flow; this is fine
            error=bidi_transform.input_transform)
    )

    validation_flow: Flow = (
        # again, flow has some input (sink) and output (source); validation_task
        # has only one sink, so no need to specify exactly validation_task.sink
        flow(validation_task)
        # validation_task has two sources; the 'ok' source is connected to two sinks: stringify and 
        # validation_side_effect;
        # if there was only one sink, we could just write .then(ok=stringify), but hI wonere there are two sinks,
        # so we put them in a list
        .then(ok=(stringify, validation_side_effect),
              # the 'error' source is connected to validation_error_side_effect
              error=validation_error_side_effect)
    )

    subnet_flow: Flow = (
        # the overall subnet flow starts from the entry point; the start doesn't have a sink,
        # it is only a source (possibly, multiple sources)
        start(entry)
        # this source is then connected to the fork; fork has only one sink, so no need to specify exactly fork.sink
        .then(fork_blah)
        .then(
            # the 'left' source of the fork is connected to the mining_flow defined above
            left=mining_flow,
            # the 'right' source of the fork is connected to the validation_flow defined above
            right=validation_flow
        )
    )

    stringify_error: Stringify[Exception] = Stringify()

    nodes = [entry, stringify, mining_task, stringify_error]

    piping = Piping()
    piping.connect(entry.source, stringify.sink)
    piping.connect(stringify.ok, mining_task.sink)
    piping.connect(mining_task.ok, entry.sink)
    piping.connect(mining_task.error, stringify_error.sink)
    piping.connect(stringify_error.ok, entry.sink)

    pipe_to_bus = PipeToBus()
    actors: list[Actor] = [node.build_actor(pipe_to_bus=pipe_to_bus) for node in nodes]

    event_bus = EventBus(piping.pipes, pipe_to_bus, actors)


"""


type Connectable = Node | Sink[Any] | Source[Any]


class Flow:
    """
    A lightweight graph representation builder, constructed with the DSL used in validator examples.

    The flow keeps track of:
    - entry_sinks: the sinks that need to be fed from upstream
    - exit_sources: the sources that can be connected further downstream
    - pipes: connections from sources to sinks
    - sources/sinks/nodes: all endpoints and components encountered in the flow
    """

    entry_sinks: NodeSinks
    exit_sources: NodeSources
    pipes: Pipes
    nodes: set[Node]
    sinks: set[Sink[Any]]
    sources: set[Source[Any]]

    def __init__(
        self,
        *,
        entry_sinks: NodeSinks,
        exit_sources: NodeSources,
        pipes: Pipes | None = None,
        nodes: set[Node] | None = None,
        sinks: set[Sink[Any]] | None = None,
        sources: set[Source[Any]] | None = None,
    ) -> None:
        self.entry_sinks = entry_sinks
        self.exit_sources = exit_sources
        self.pipes = pipes or Pipes(set)
        self.nodes = nodes or set()
        self.sinks = sinks or set()
        self.sources = sources or set()

    @classmethod
    def from_connectable(cls, connectable: Connectable) -> Flow:
        node: Node
        match connectable:
            case Sink() as sink:
                node = SinkNode(sink)
            case Source() as source:
                node = SourceNode(source)
            case Node() as node:
                pass
        sinks = node.sinks()
        sources = node.sources()

        flow_object = cls(
            entry_sinks=sinks,
            exit_sources=sources,
            pipes=Pipes(),
            nodes={node},
            sinks=set(sinks.sinks.values()),
            sources=set(sources.sources.values()),
        )
        return flow_object

    def then(self, *targets: Connectable | Flow, **routes: Connectable | Flow | Iterable[Connectable | Flow]) -> Flow:
        if not targets and not routes:
            raise FlowMisconfiguredException(
                "expected continuation of the flow as either positional or keyword parameters"
            )
        if targets and routes:
            raise FlowMisconfiguredException(
                "expected continuation of the flow as either positional or keyword paramters"
            )

        if targets:
            source = self.exit_sources.default_source
            if source is None:
                raise FlowMisconfiguredException(
                    "No default exit source to connect the continuation to the provided sinks: "
                    f"exit_sources={self.exit_sources}"
                )

            continuation_exit_sources: NodeSources | None = None

            for target in targets:
                target_flow = Flow._as_flow(target)
                if target_flow.entry_sinks.default_sink is None:
                    raise FlowMisconfiguredException(
                        f"No default entry sink to connect the source to the provided flow: target_flow={target_flow}"
                    )

                self._absorb(target_flow)
                self._connect(source, target_flow.entry_sinks.default_sink)

                if target_flow.exit_sources.sources:
                    if continuation_exit_sources is not None:
                        raise FlowMisconfiguredException(
                            "multiple continuation targets define exit sources: "
                            f"{continuation_exit_sources} and {target_flow.exit_sources}"
                        )
                    continuation_exit_sources = target_flow.exit_sources

            # continuation only if there is exactly one source among the target flows
            self.exit_sources = continuation_exit_sources or NodeSources(sources={})
            return self

        elif routes:
            for source_str, target in routes.items():
                source_name = SourceName(source_str)
                if source_name not in self.exit_sources.sources:
                    raise FlowMisconfiguredException(
                        f"Unexpected connection from {source_name}; available sources: {self.exit_sources.sources}"
                    )
                source = self.exit_sources.sources[source_name]

                targets_to_connect: Iterable[Connectable | Flow] = (
                    list(target) if isinstance(target, Iterable) else [target]
                )

                for target_item in targets_to_connect:
                    target_flow = Flow._as_flow(target_item)
                    if target_flow.entry_sinks.default_sink is None:
                        raise FlowMisconfiguredException(
                            "No default entry sink to connect the source to the provided flow: "
                            f"target_flow={target_flow}"
                        )

                    self._absorb(target_flow)
                    self._connect(source, target_flow.entry_sinks.default_sink)

            # no direct continuation after a fork
            self.exit_sources = NodeSources(sources={})
            return self

        else:
            raise AssertionError("should be unreachable code")

    def _connect[T](self, source: Source[T], sink: Sink[T]) -> None:
        self.sources.add(source)
        self.sinks.add(sink)
        self.pipes[source].add(sink)

    def _absorb(self, target_flow: Flow) -> None:
        self.nodes |= target_flow.nodes
        self.sinks |= target_flow.sinks
        self.sources |= target_flow.sources

        for source, sinks in target_flow.pipes.items():
            self.pipes[source].update(sinks)

    @staticmethod
    def _as_flow(component: Connectable | Flow) -> Flow:
        if isinstance(component, Flow):
            return component
        else:
            return Flow.from_connectable(component)
