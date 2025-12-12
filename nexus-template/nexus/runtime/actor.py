import itertools
from abc import ABC, abstractmethod
from collections.abc import Callable
from threading import Thread
from typing import Any, NewType, override

from nexus.context_store import ContextId
from nexus.logging_utils import get_logger
from nexus.piping.dsl import Sink, Transform
from nexus.runtime.events import PipeFromBus, PipeToBus, ReceiveEvent, SendEvent, StopActorEvent

logger = get_logger("Actor")

ActorId = NewType("ActorId", str)

EventHandler = Callable[[ReceiveEvent[Any]], None]

class Actor(ABC):
    actor_id: ActorId
    pipe_from_bus: PipeFromBus  # transport for incoming events
    pipe_to_bus: PipeToBus  # transport for outgoing events

    actor_counter: itertools.count[int] = itertools.count()

    @classmethod
    def default_actor_id(cls, name: str) -> ActorId:
        return ActorId(f"{cls.__name__}-{name}-{next(Actor.actor_counter)}")

    def __init__(self, *, name: str, pipe_to_bus: PipeToBus) -> None:
        self.actor_id = Actor.default_actor_id(name)
        self.pipe_from_bus = PipeFromBus()
        self.pipe_to_bus = pipe_to_bus

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
                handler: EventHandler | None = self.handlers().get(event.target, None)
                if handler:
                    try:
                        handler(event)
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


class TransformActor[From, To](Actor, ABC):
    spec: Transform[From, To]

    def __init__(self, spec: Transform[From, To], pipe_to_bus: PipeToBus) -> None:
        super().__init__(name=spec.name, pipe_to_bus=pipe_to_bus)
        self.spec = spec

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {
            self.spec.sink: self.handle
        }

    def handle(self, event: ReceiveEvent[From]) -> None:
        assert event.target == self.spec.sink
        try:
            output_payload = self._transform(event.ctx, event.payload)
            self.pipe_to_bus.put(
                SendEvent(ctx=event.ctx, source=self.spec.ok, payload=output_payload)
            )
        except Exception as exception:
            self.pipe_to_bus.put(
                SendEvent(ctx=event.ctx, source=self.spec.error, payload=exception)
            )

    @abstractmethod
    def _transform(self, ctx: ContextId, payload: From) -> To:
        pass
