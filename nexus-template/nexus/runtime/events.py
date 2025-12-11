import queue
from dataclasses import dataclass
from typing import Any

from nexus.context_store import ContextId
from nexus.piping.dsl import Sink, SinkId, Source, SourceId


@dataclass
class Event[T]:
    ctx: ContextId
    payload: T


@dataclass
class SendEvent[T](Event[T]):
    source: Source[T]


@dataclass
class ReceiveEvent[T](Event[T]):
    target: Sink[T]


# we'd need some proper control events someday...
_STOP_CTX = ContextId("stop")
_STOP_SOURCE = Source[None](SourceId("stop"))
_STOP_SINK = Sink[None](SinkId("stop"))


class StopBusEvent(SendEvent[None]):
    def __init__(self) -> None:
        super().__init__(ctx=_STOP_CTX, payload=None, source=_STOP_SOURCE)


class StopActorEvent(ReceiveEvent[None]):
    def __init__(self) -> None:
        super().__init__(ctx=_STOP_CTX, payload=None, target=_STOP_SINK)


ToBus = queue.Queue[SendEvent[Any]]
FromBus = queue.Queue[ReceiveEvent[Any]]
