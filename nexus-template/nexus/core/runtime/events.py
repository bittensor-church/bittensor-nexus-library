import queue
from dataclasses import dataclass
from typing import Any

from ..dsl.nodes import Sink, Source
from .context_store_types import ContextId

# I somehow feel these should use deep copy on initialization to ~enforce immutability


@dataclass
class Event[T]:
    ctx_id: ContextId
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
        super().__init__(ctx_id=_STOP_CTX, source=_STOP_SOURCE, payload=None)


class StopActorEvent(ReceiveEvent[None]):
    def __init__(self) -> None:
        super().__init__(ctx_id=_STOP_CTX, target=_STOP_SINK, payload=None)


PipeToBus = queue.Queue[SendEvent[Any]]
PipeFromBus = queue.Queue[ReceiveEvent[Any]]

type MessagesToSend = SendEvent[Any] | tuple[SendEvent[Any], ...]