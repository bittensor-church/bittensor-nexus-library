import itertools
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, NewType, override

from nexus.context_store import ContextId
from nexus.logging_utils import get_logger
from nexus.piping.dsl import Sink, Transform
from nexus.runtime.events import FromBus, ReceiveEvent, SendEvent, StopActorEvent, ToBus

logger = get_logger("Actor")

ActorId = NewType("ActorId", str)

EventHandler = Callable[[ReceiveEvent[Any]], None]

class Actor(ABC):
    actor_id: ActorId
    from_bus: FromBus  # transport for incoming events
    to_bus: ToBus  # transport for outgoing events

    actor_counter: itertools.count[int] = itertools.count()

    @classmethod
    def default_actor_id(cls, name: str) -> ActorId:
        return ActorId(f"{cls.__name__}-{name}-{next(Actor.actor_counter)}")

    def __init__(self, name: str, to_bus: ToBus) -> None:
        self.actor_id = Actor.default_actor_id(name)
        self.to_bus = to_bus

    def loop(self) -> None:
        while True:
            event: ReceiveEvent[Any] = self.from_bus.get()
            if isinstance(event, StopActorEvent):
                logger.info(f"Stop event received in actor: {self.actor_id}; stopping loop.")
                break
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

    @abstractmethod
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        pass


class ActorBuilder(ABC):
    @abstractmethod
    def build_actor(self, *, to_bus: ToBus) -> Actor:
        pass


class TransformActor[From, To](Actor, ABC):
    spec: Transform[From, To]

    def __init__(self, *, name: str, spec: Transform[From, To], to_bus: ToBus) -> None:
        super().__init__(name, to_bus)
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
            self.to_bus.put(
                SendEvent(ctx=event.ctx, source=self.spec.source, payload=output_payload)
            )
        except Exception as e:
            logger.error(
                f"Error in TransformActor {self.actor_id} while processing event: {event}",
                exc_info=e,
            )

    @abstractmethod
    def _transform(self, ctx: ContextId, payload: From) -> To:
        pass
