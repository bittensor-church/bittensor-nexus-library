import itertools
from collections import defaultdict
from dataclasses import dataclass
from typing import TypeVar, NewType

T = TypeVar("T")

SourceId = NewType("SourceId", str)
SinkId = NewType("SinkId", str)

endpoints_counter: itertools.count = itertools.count()


class Source[T]:
    """
        A logical endpoint for data production.
    """
    source_id: SourceId

    def __init__(self, source_id: SourceId):
        self.source_id = source_id


class Sink[T]:
    """
        A logical endpoint for data consumption.
    """
    sink_id: SinkId

    def __init__(self, sink_id: SinkId):
        self.sink_id = sink_id


class Transform[From, To]:
    sink: Sink[From]
    source: Source[To]
    """
        A logical data processing unit that consumes data from a Sink and produces data to a Source.
    """

    def __init__(self, name: str):
        self.source = Source[To](SourceId(f"{name}-source"))
        self.sink = Sink[From](SinkId(f"{name}-sink"))


Sources = set[Source]
Sinks = set[Sink]
Pipes = defaultdict[Source, set[Sink]]


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

    def connect[T](self, source: Source[T], sink: Sink[T]):
        self.pipes[source].add(sink)
