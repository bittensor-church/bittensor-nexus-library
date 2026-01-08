from __future__ import annotations

import itertools
import traceback
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, NewType, TypeVar, override

T = TypeVar("T")

# globally unique IDs for Sources and Sinks, to be used to identify them in the system
SourceId = NewType("SourceId", str)
SinkId = NewType("SinkId", str)

# locally unique IDs for Sources and Sinks, to be used within a context of a specific
# contraption that owns them
SourceName = NewType("SourceName", str)
SinkName = NewType("SinkName", str)


@dataclass
class NodeSources:
    sources: dict[SourceName, Source[Any]]
    default_source: Source[Any] | None = None

    def __post_init__(self) -> None:
        assert self.default_source is None or self.default_source in self.sources.values(), \
            f"primary_source {self.default_source!r} must be None or a key in sources; " \
            f"available: {list(self.sources)}"
        if len(self.sources) == 1:
            self.default_source = next(iter(self.sources.values()))

    def empty(self) -> bool:
        return bool(self.sources)


@dataclass
class NodeSinks:
    sinks: dict[SinkName, Sink[Any]]
    default_sink: Sink[Any] | None = None

    def __post_init__(self) -> None:
        assert self.default_sink is None or self.default_sink in self.sinks, \
            f"primary_sink {self.default_sink!r} must be None or a key in sinks; " \
            f"available: {list(self.sinks.keys())}"
        if len(self.sinks) == 1:
            self.default_sink = next(iter(self.sinks.values()))


class Node(ABC):
    """
        any element of the processing graph is a Node
    """

    @abstractmethod
    def sinks(self) -> NodeSinks:
        pass

    @abstractmethod
    def sources(self) -> NodeSources:
        pass


class HasGlobalId:
    id_counter: itertools.count[int] = itertools.count()
    global_ids: dict[str, traceback.StackSummary] = {}  # to track where IDs were created

    gid: str

    def __init__(self, gid_prefix: str | None = None) -> None:
        super().__init__()
        self.gid = f'{gid_prefix or self.__class__.__name__}-{next(HasGlobalId.id_counter)}'
        assert self.gid not in HasGlobalId.global_ids, \
            f"Global ID collision: {self.gid} created previously in\n" \
            f"{''.join(traceback.format_list(HasGlobalId.global_ids[self.gid]))}"
        HasGlobalId.global_ids[self.gid] = traceback.extract_stack()


class Source[T](HasGlobalId, Node):
    """
        A logical endpoint for data production.
    """

    @override
    def sources(self) -> NodeSources:
        return NodeSources(sources={
            SourceName("self"): self
        })

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks(sinks={})


class Sink[T](HasGlobalId, Node):
    """
        A logical endpoint for data consumption.
    """

    @override
    def sources(self) -> NodeSources:
        return NodeSources(sources={})

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks({
            SinkName("self"): self
        })


class Fork[From, ToLeft, ToRight](HasGlobalId, Node):
    """
        A logical data processing unit that forks data from a Sink to two Sources.
    """

    sink: Sink[From]
    left: Source[ToLeft]
    right: Source[ToRight]

    def __init__(self, gid_prefix: str | None = None):
        super().__init__(gid_prefix=gid_prefix)
        self.sink = Sink[From](gid_prefix=f"{self.gid}-sink")
        self.left = Source[ToLeft](gid_prefix=f"{self.gid}-left-source")
        self.right = Source[ToRight](gid_prefix=f"{self.gid}-right-source")

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks(sinks={
            SinkName("sink"): self.sink,
        })

    @override
    def sources(self) -> NodeSources:
        return NodeSources(sources={
            SourceName("left"): self.left,
            SourceName("right"): self.right,
        })


class Transform[From, To](Fork[From, To, Exception], Node):
    # convenient aliases
    ok: Source[To]
    error: Source[Exception]
    """
        A logical data processing unit that consumes data from a Sink and produces data to the ok Source or
        reports errors to the error Source.
        Transform is a Fork really, but for the time being it's clearer to have it as a separate concept.
    """

    def __init__(self, gid_prefix: str | None = None):
        super().__init__(gid_prefix=gid_prefix)

        self.ok = self.left
        self.error = self.right

    @override
    def sources(self) -> NodeSources:
        return NodeSources(
            sources={
                SourceName("ok"): self.ok,
                SourceName("error"): self.error,
            },
            default_source=self.ok)


class DoubleTransform[InputFrom, InputTo, OutputFrom, OutputTo](HasGlobalId, Node):
    """
        A logical data processing unit that is a two-way Transform
        - the first Transform converts InputFrom to InputTo
        - the second Transform converts InputTo to OutputTo
    """
    input_transform: Transform[InputFrom, InputTo]
    output_transform: Transform[OutputFrom, OutputTo]

    # convenient aliases
    input_sink: Sink[InputFrom]
    input_ok: Source[InputTo]
    input_error: Source[Exception]

    output_sink: Sink[OutputFrom]
    output_ok: Source[OutputTo]
    output_error: Source[Exception]

    def __init__(self, gid_prefix: str | None = None):
        super().__init__(gid_prefix=gid_prefix)
        self.input_transform = Transform[InputFrom, InputTo](gid_prefix=f"{self.gid}-input-transform")
        self.output_transform = Transform[OutputFrom, OutputTo](gid_prefix=f"{self.gid}-output-transform")

        self.input_sink = self.input_transform.sink
        self.input_ok = self.input_transform.ok
        self.input_error = self.input_transform.error

        self.output_sink = self.output_transform.sink
        self.output_ok = self.output_transform.ok
        self.output_error = self.output_transform.error

    def sources(self) -> NodeSources:
        return NodeSources(
            sources={
                SourceName("input_ok"): self.input_ok,
                SourceName("input_error"): self.input_error,
                SourceName("output_ok"): self.output_ok,
                SourceName("output_error"): self.output_error,
            }
        )

    def sinks(self) -> NodeSinks:
        return NodeSinks(
            sinks={
                SinkName("input_sink"): self.input_sink,
                SinkName("output_sink"): self.output_sink,
            }
        )


Sources = set[Source[Any]]
Sinks = set[Sink[Any]]
Pipes = defaultdict[Source[Any], set[Sink[Any]]]
