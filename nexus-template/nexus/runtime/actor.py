import itertools
from abc import abstractmethod, ABC
from typing import NewType, Callable

from nexus import get_logger
from nexus.context_store import ContextId
from nexus.piping.dsl import Transform, Sink
from nexus.runtime.event_bus import ToBus, FromBus, ReceiveEvent, SendEvent, StopEvent, StopActorEvent

logger = get_logger("Actor")

ActorId = NewType("ActorId", str)


class Actor(ABC):
    actor_id: ActorId
    from_bus: FromBus  # transport for incoming events
    to_bus: ToBus  # transport for outgoing events

    actor_counter: itertools.count = itertools.count()

    @classmethod
    def default_actor_id(cls, name):
        return ActorId(f"{cls.__name__}-{name}-{next(Actor.actor_counter)}")

    def __init__(self, name: str, to_bus: ToBus) -> None:
        self.actor_id = Actor.default_actor_id(name)
        self.to_bus = to_bus

    def loop(self):
        while True:
            event: ReceiveEvent = self.from_bus.get()
            if event is StopActorEvent:
                logger.info(f"Stop event received in actor: {self.actor_id}; stopping loop.")
                break
            handler = self.handlers().get(event.target, None)
            if handler:
                handler(event)
            else:
                logger.error(f"No handler found for sink: {event.target} in actor: {self.actor_id}")

    @abstractmethod
    def handlers(self) -> dict[Sink, Callable[[ReceiveEvent], None]]:
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

    def handlers(self) -> dict[Sink, Callable[[ReceiveEvent], None]]:
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
            logger.error(f"Error in TransformActor {self.actor_id} while processing event: {event}", e)

    @abstractmethod
    def _transform(self, ctx: ContextId, payload: From) -> To:
        pass
