from __future__ import annotations

import queue
from dataclasses import dataclass

from nexus.context_store import ContextId
from nexus.logging_utils import get_logger
from nexus.piping.dsl import Pipes, Source, Sink, SourceId, SinkId
from nexus.runtime.actor import Actor

logger = get_logger(__name__)


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
StopBusEvent = SendEvent(ctx=ContextId("stop"), source=Source(SourceId("stop")), payload=None)
StopActorEvent = ReceiveEvent(ctx=ContextId("stop"), target=Sink(SinkId("stop")), payload=None)

ToBus = queue.Queue[SendEvent]
FromBus = queue.Queue[ReceiveEvent]


class EventBus:
    connections: Pipes
    input_pipe: ToBus
    sinks: dict[Sink, Actor]

    def __init__(self, connections: Pipes, input_pipe: ToBus, sinks: dict[Sink, Actor]) -> None:
        self.connections = connections
        self.sinks = sinks
        self.input_pipe = input_pipe

    def stop(self):
        self.input_pipe.put(StopBusEvent)

    def loop(self):
        while True:
            event: SendEvent = self.input_pipe.get()
            if event is StopBusEvent:
                logger.info("Stop event received in EventBus; stopping loop.")
                for sink in self.sinks.values():
                    sink.from_bus.put(StopActorEvent)
                break
            events_passed = 0
            for sink in self.connections[event.source]:
                logger.debug(
                    f"Sending event from {event.source} to {sink} with payload: {event.payload}")
                self.sinks[sink].from_bus.put(
                    ReceiveEvent(ctx=event.ctx, target=sink, payload=event.payload))
                events_passed += 1

            if events_passed == 0:
                logger.error(f"No connections found for source: {event.source}; connections: {self.connections}")
