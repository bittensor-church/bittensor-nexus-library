import itertools
from abc import ABC, abstractmethod
from collections.abc import Callable
from threading import Thread
from typing import Any, NewType

from nexus.logging_utils import get_logger
from .context_store import ContextStore, Context

from ..dsl.nodes import Sink
from .events import PipeFromBus, PipeToBus, ReceiveEvent, StopActorEvent

logger = get_logger("Actor")

ActorId = NewType("ActorId", str)

EventHandler = Callable[[Context, ReceiveEvent[Any]], None]


class Actor(ABC):
    actor_id: ActorId
    pipe_from_bus: PipeFromBus  # transport for incoming events
    pipe_to_bus: PipeToBus  # transport for outgoing events
    context_store: ContextStore

    actor_counter: itertools.count[int] = itertools.count()

    @classmethod
    def default_actor_id(cls, name: str) -> ActorId:
        return ActorId(f"{cls.__name__}-{name}-{next(Actor.actor_counter)}")

    def __init__(self, *, name: str, pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        self.actor_id = Actor.default_actor_id(name)
        self.pipe_from_bus = PipeFromBus()
        self.pipe_to_bus = pipe_to_bus
        self.context_store = context_store

    def run_loop(self) -> Thread:
        t: Thread = Thread(target=self._loop, daemon=True, name=f"ActorLoop-{self.actor_id}")
        t.start()
        return t

    def _loop(self) -> None:
        while True:
            event: ReceiveEvent[Any] = self.pipe_from_bus.get()
            if isinstance(event, StopActorEvent):
                logger.info(f"Stop event received in actor: {self.actor_id}; stopping loop.")
                self.pipe_from_bus.task_done()
                break
            else:
                context: Context = self.context_store.get_context(event.ctx_id)
                handler: EventHandler | None = self.handlers().get(event.target, None)
                if handler:
                    try:
                        handler(context, event)
                    except Exception as exc:
                        logger.error(
                            f"Error while handling event {event} in actor {self.actor_id} for target {event.target}",
                            exc_info=exc,
                        )
                else:
                    logger.error(f"No handler found for sink: {event.target} in actor: {self.actor_id}")
                self.pipe_from_bus.task_done()

    @abstractmethod
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        pass


class ActorBuilder(ABC):
    @abstractmethod
    def build_actor(self, *, pipe_to_bus: PipeToBus) -> Actor:
        pass
