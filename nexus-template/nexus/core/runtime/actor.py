import itertools
from abc import ABC, abstractmethod
from collections.abc import Callable
from threading import Thread
from typing import Any, NewType

from nexus.logging_utils import get_logger

from ..dsl.nodes import Sink
from .context_store import Context, ContextStore
from .events import MessagesToSend, PipeFromBus, PipeToBus, ReceiveEvent, SendEvent, StopActorEvent

logger = get_logger("Actor")

ActorId = NewType("ActorId", str)

EventHandler = Callable[[Context, ReceiveEvent[Any]], MessagesToSend]


class Actor(ABC):
    """
    Base runtime processing unit.

    Actor instances are one-off: their event loop can be started only once.
    """

    actor_id: ActorId
    context_store: ContextStore
    pipe_from_bus: PipeFromBus  # transport for incoming events
    _pipe_to_bus: PipeToBus  # transport for outgoing events
    thread: Thread | None

    actor_counter: itertools.count[int] = itertools.count()

    @classmethod
    def default_actor_id(cls, name: str) -> ActorId:
        return ActorId(f"{cls.__name__}-{name}-{next(Actor.actor_counter)}")

    def __init__(self, *, name: str, pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        self.actor_id = Actor.default_actor_id(name)
        self.pipe_from_bus = PipeFromBus()
        self._pipe_to_bus = pipe_to_bus
        self.context_store = context_store
        self.thread = None

    def run_loop(self) -> Thread:
        if self.thread is not None:
            raise RuntimeError(f"Actor {self.actor_id} is already running in thread {self.thread.name}")
        self.thread = Thread(target=self._loop, daemon=True, name=f"ActorLoop-{self.actor_id}")
        self.thread.start()
        return self.thread

    def _loop(self) -> None:
        while True:
            event_to_handle: ReceiveEvent[Any] = self.pipe_from_bus.get()
            events_produced_by_the_handler: MessagesToSend = ()
            if isinstance(event_to_handle, StopActorEvent):
                logger.info(f"Stop event received in actor: {self.actor_id}; stopping loop.")
                self.pipe_from_bus.task_done()
                break
            else:
                handler: EventHandler | None = self.handlers().get(event_to_handle.target, None)
                if handler:
                    try:
                        with self.context_store.get_context(event_to_handle.ctx_id) as context:
                            events_produced_by_the_handler = handler(context, event_to_handle)
                    except Exception as exc:
                        logger.error(
                            "Error while handling event %s in actor %s for target %s",
                            event_to_handle,
                            self.actor_id,
                            event_to_handle.target,
                            exc_info=exc,
                        )
                else:
                    logger.error(f"No handler found for sink: {event_to_handle.target} in actor: {self.actor_id}")

                match events_produced_by_the_handler:
                    case SendEvent() as event:
                        self._pipe_to_bus.put(event)
                    case tuple() as events:
                        for event_to_send in events:
                            self._pipe_to_bus.put(event_to_send)
                    case _:  # pyright: ignore[reportUnnecessaryComparison]
                        # Defensive fallback for strict pyright, which can conservatively
                        # retain an "Unknown" branch for this match.
                        raise AssertionError(
                            f"Unexpected handler return type: {type(events_produced_by_the_handler)!r}"
                        )
                self.pipe_from_bus.task_done()

    @abstractmethod
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        pass


class ActorBuilder(ABC):
    @abstractmethod
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        pass
