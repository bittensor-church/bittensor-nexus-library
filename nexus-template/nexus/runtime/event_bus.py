from __future__ import annotations

from nexus.logging_utils import get_logger
from nexus.piping.dsl import Pipes, Sink
from nexus.runtime.actor import Actor
from nexus.runtime.events import ToBus, StopBusEvent, SendEvent, StopActorEvent, ReceiveEvent

logger = get_logger(__name__)


class EventBus:
    connections: Pipes
    input_pipe: ToBus
    sinks: dict[Sink, Actor]

    def __init__(self, connections: Pipes, input_pipe: ToBus, sinks: dict[Sink, Actor]) -> None:
        self.connections = connections
        self.sinks = sinks
        self.input_pipe = input_pipe

    def stop(self):
        self.input_pipe.put(StopBusEvent())

    def loop(self):
        while True:
            event: SendEvent = self.input_pipe.get()
            if isinstance(event, StopBusEvent):
                logger.info("Stop event received in EventBus; stopping loop.")
                for sink in self.sinks.values():
                    sink.from_bus.put(StopActorEvent())
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
