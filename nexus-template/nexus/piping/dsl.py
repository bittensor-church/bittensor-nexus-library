from __future__ import annotations

import itertools
from collections import defaultdict
from dataclasses import dataclass
from typing import TypeVar, NewType, Any

T = TypeVar("T")


SourceId = NewType("SourceId", str)
SinkId = NewType("SinkId", str)

endpoints_counter: itertools.count[int] = itertools.count()


class Source[T]:
    """
        A logical endpoint for data production.
    """
    source_id: SourceId

    def __init__(self, source_id: SourceId):
        self.source_id = source_id

    @classmethod
    def with_name(cls, name: str) -> SourceId:
        return SourceId(f"{name}-source")


class Sink[T]:
    """
        A logical endpoint for data consumption.
    """
    sink_id: SinkId

    def __init__(self, sink_id: SinkId):
        self.sink_id = sink_id

    @classmethod
    def with_name(cls, name: str) -> SinkId:
        return SinkId(f"{name}-sink")

class Transform[From, To]:
    sink: Sink[From]
    source: Source[To]
    """
        A logical data processing unit that consumes data from a Sink and produces data to a Source.
    """

    def __init__(self, name: str):
        self.source = Source[To](Source.with_name(name))
        self.sink = Sink[From](Sink.with_name(name))


Sources = set[Source[Any]]
Sinks = set[Sink[Any]]
Pipes = defaultdict[Source[Any], set[Sink[Any]]]  # we loose type info here for simplicity


@dataclass
class Piping:
    """
        DSL for defining data flow connections between Sources and Sinks.
    """
    pipes: Pipes
    sources: Sources
    sinks: Sinks

    def __init__(self):
        self.pipes = Pipes(set)

    def connect[T](self, source: Source[T], sink: Sink[T]) -> None:
        self.pipes[source].add(sink)
