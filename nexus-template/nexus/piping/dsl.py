from __future__ import annotations

import itertools
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, NewType, TypeVar

T = TypeVar("T")


SourceId = NewType("SourceId", str)
SinkId = NewType("SinkId", str)


class Named:
    name_counter: itertools.count[int] = itertools.count()

    name: str

    def __init__(self, name: str | None = None) -> None:
        super().__init__()
        if name is None:
            name = f'{self.__class__.__name__}-{next(Named.name_counter)}'
        self.name = name


class Source[T](Named):
    """
        A logical endpoint for data production.
    """

    source_counter: itertools.count[int] = itertools.count()

    source_id: SourceId

    def __init__(self, name: str | None = None) -> None:
        super().__init__(name=name)
        self.source_id = SourceId(f"{self.name}-source-{next(Source.source_counter)}")


class Sink[T](Named):
    """
        A logical endpoint for data consumption.
    """
    sink_counter: itertools.count[int] = itertools.count()

    sink_id: SinkId

    def __init__(self, name: str | None = None) -> None:
        super().__init__(name=name)
        self.sink_id = SinkId(f"{self.name}-sink-{next(Sink.sink_counter)}")


class Transform[From, To](Named):
    """
        A logical data processing unit that consumes data from a Sink and produces data to the ok Source or
        reports errors to the error Source.
    """
    sink: Sink[From]
    ok: Source[To]
    error: Source[Exception]

    def __init__(self, name: str | None = None):
        super().__init__(name=name)
        self.sink = Sink[From](name=f"{name}")
        self.ok = Source[To](name=f"{name}-ok")
        self.error = Source[Exception](name=f"{name}-error")


Sources = set[Source[Any]]
Sinks = set[Sink[Any]]
Pipes = defaultdict[Source[Any], set[Sink[Any]]]  # we lose type info here for simplicity


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
