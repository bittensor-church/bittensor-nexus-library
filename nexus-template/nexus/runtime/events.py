import queue
from dataclasses import dataclass

from nexus.context_store import ContextId
from nexus.piping.dsl import Source, Sink


@dataclass
class Event[T]:
    ctx: ContextId
    payload: T


@dataclass
class SendEvent[T](Event[T]):
    source: Source


@dataclass
class ReceiveEvent[T](Event[T]):
    target: Sink


# we'd need some proper control events someday...
class StopBusEvent(SendEvent[None]):
    pass


class StopActorEvent(ReceiveEvent[None]):
    pass


ToBus = queue.Queue[SendEvent]
FromBus = queue.Queue[ReceiveEvent]
