import itertools
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from threading import Thread
from typing import Any, NewType

from nexus.logging_utils import get_logger
from .context_store import ContextStore, Context

from ..dsl.nodes import Sink
from .events import PipeFromBus, PipeToBus, ReceiveEvent, SendEvent, StopActorEvent

logger = get_logger("Actor")

ActorId = NewType("ActorId", str)

EventHandler = Callable[[Context, ReceiveEvent[Any]], Iterable[SendEvent[Any]]]


class Actor(ABC):
    actor_id: ActorId
    context_store: ContextStore
    pipe_from_bus: PipeFromBus  # transport for incoming events
    __pipe_to_bus: PipeToBus  # transport for outgoing events

    actor_counter: itertools.count[int] = itertools.count()

    @classmethod
    def default_actor_id(cls, name: str) -> ActorId:
        return ActorId(f"{cls.__name__}-{name}-{next(Actor.actor_counter)}")

    def __init__(self, *, name: str, pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        self.actor_id = Actor.default_actor_id(name)
        self.pipe_from_bus = PipeFromBus()
        self.__pipe_to_bus = pipe_to_bus
        self.context_store = context_store

    def run_loop(self) -> Thread:
        t: Thread = Thread(target=self._loop, daemon=True, name=f"ActorLoop-{self.actor_id}")
        t.start()
        return t

    def _loop(self) -> None:
        while True:
            event: ReceiveEvent[Any] = self.pipe_from_bus.get()
            events_produced_by_the_handler = ()
            if isinstance(event, StopActorEvent):
                logger.info(f"Stop event received in actor: {self.actor_id}; stopping loop.")
                self.pipe_from_bus.task_done()
                break
            else:
                handler: EventHandler | None = self.handlers().get(event.target, None)
                if handler:
                    try:
                        with self.context_store.get_context(event.ctx_id) as context:
                            events_produced_by_the_handler = handler(context, event)
                    except Exception as exc:
                        logger.error(
                            f"Error while handling event {event} in actor {self.actor_id} for target {event.target}",
                            exc_info=exc,
                        )
                else:
                    logger.error(f"No handler found for sink: {event.target} in actor: {self.actor_id}")

                for event_to_send in events_produced_by_the_handler:
                    self.__pipe_to_bus.put(event_to_send)
                self.pipe_from_bus.task_done()

    @abstractmethod
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        pass


class ActorBuilder(ABC):
    @abstractmethod
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        pass
