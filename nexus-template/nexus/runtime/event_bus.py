import logging
from typing import Any

from nexus.logging_utils import get_logger
from nexus.piping.dsl import Sink, Pipes
from nexus.runtime.actor import Actor
from nexus.runtime.events import ReceiveEvent, SendEvent, StopActorEvent, StopBusEvent, ToBus

logger: logging.Logger = get_logger(__name__)


class EventBus:
    connections: Pipes
    input_pipe: ToBus
    sinks: dict[Sink[Any], Actor]

    def __init__(self, connections: Pipes, input_pipe: ToBus, sinks: dict[Sink[Any], Actor]) -> None:
        self.connections = connections
        self.sinks = sinks
        self.input_pipe = input_pipe

    def stop(self) -> None:
        self.input_pipe.put(StopBusEvent())

    def loop(self) -> None:
        while True:
            event: SendEvent[Any] = self.input_pipe.get()
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
