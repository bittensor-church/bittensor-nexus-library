import logging
from threading import Thread
from typing import Any

from nexus.logging_utils import get_logger
from .actor import Actor
from .context_store import ContextStore
from .events import PipeToBus, ReceiveEvent, SendEvent, StopActorEvent, StopBusEvent
from ..dsl.nodes import Pipes, Sink

logger: logging.Logger = get_logger(__name__)


class EventBus:
    connections: Pipes
    input_pipe: PipeToBus
    unconsumed_events_sink: Actor
    sinks: dict[Sink[Any], Actor]
    context_store: ContextStore

    def __init__(
        self, connections: Pipes, input_pipe: PipeToBus, actors: list[Actor], context_store: ContextStore
    ) -> None:
        self.connections = connections
        self.sinks = {sink: actor for actor in actors for sink in actor.handlers().keys()}
        self.input_pipe = input_pipe
        self.context_store = context_store
        for actor in actors:
            assert actor._Actor__pipe_to_bus is self.input_pipe, (
                f"Actor {actor.actor_id} pipe_to_bus does not match EventBus input_pipe."
            )

    def request_stop(self) -> None:
        self.input_pipe.put(StopBusEvent())

    def run_loop(self) -> Thread:
        t: Thread = Thread(target=self._loop, daemon=True, name="EventBusLoop")
        t.start()
        return t

    def _loop(self) -> None:
        while True:
            event: SendEvent[Any] = self.input_pipe.get()
            if isinstance(event, StopBusEvent):
                logger.info("Stop event received in EventBus; stopping loop.")
                for sink in self.sinks.values():
                    sink.pipe_from_bus.put(StopActorEvent())
                self.input_pipe.task_done()
                break
            else:
                # update the context
                with self.context_store.get_context(event.ctx_id) as context:
                    context.append_message(event.source, event.payload)

                self.pass_message_downstream(event)
                self.input_pipe.task_done()

    def pass_message_downstream(self, event: SendEvent) -> None:
        """
        Actual message distribution logic. Recovery from the context store
        means we rebuild the contexts and then replay the messages using this function.
        """
        events_passed_downstream = 0
        for sink in self.connections[event.source]:
            logger.debug(f"Sending event from {event.source} to {sink} with payload: {event.payload}")
            self.sinks[sink].pipe_from_bus.put(ReceiveEvent(ctx_id=event.ctx_id, target=sink, payload=event.payload))
            events_passed_downstream += 1

        if events_passed_downstream == 0:
            logger.error(f"No connections found for source: {event.source}; connections: {self.connections}")
