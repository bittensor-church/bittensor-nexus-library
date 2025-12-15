import queue
from dataclasses import dataclass
from typing import Any

from nexus.context_store import ContextId

from ..dsl.nodes import Sink, Source


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
_STOP_SOURCE = Source[None]("stop-source")
_STOP_SINK = Sink[None]("stop-sink")


class StopBusEvent(SendEvent[None]):
    def __init__(self) -> None:
        super().__init__(ctx=_STOP_CTX, payload=None, source=_STOP_SOURCE)


class StopActorEvent(ReceiveEvent[None]):
    def __init__(self) -> None:
        super().__init__(ctx=_STOP_CTX, payload=None, target=_STOP_SINK)


PipeToBus = queue.Queue[SendEvent[Any]]
PipeFromBus = queue.Queue[ReceiveEvent[Any]]
