from __future__ import annotations

import queue
from dataclasses import dataclass
from typing import Any, TypeVar

from .actor import Actor
from .logging_utils import get_logger

logger = get_logger(__name__)


T = TypeVar("T")


@dataclass
class Source[T]:
    name: str


@dataclass
class Sink[T]:
    name: str
    queue: queue.Queue[T]


@dataclass
class Event[T]:
    source: Source[T]
    payload: T


class ActorConnection[T]:
    source: Source[T]
    sink: Sink[T]

    def __init__(self, source: Source[T], sink: Sink[T]):
        self.source = source
        self.sink = sink


ActorConnections = list[ActorConnection[Any]]  # pyright: ignore[reportExplicitAny]


class PipingBuilder:
    connections: ActorConnections

    def __init__(self):
        self.connections = []

    def connect[T](self, source: Source[T], sink: Sink[T]):
        self.connections.append(ActorConnection(source, sink))

    def build_piping(self) -> Piping:
        return Piping("Piping", self.connections)


class Piping(Actor):
    connections: ActorConnections

    def __init__(self, name: str, connections: ActorConnections):
        super().__init__(name)
        self.connections = connections

    def send(self, event: Event[T]):
        for connection in self.connections:
            if connection.source == event.source:
                logger.debug(f"Sending event from {connection.source.name} to {connection.sink.name} with payload: {event.payload}")
                connection.sink.queue.put(event.payload)
