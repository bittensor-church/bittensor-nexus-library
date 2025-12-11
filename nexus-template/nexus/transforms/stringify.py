from nexus.context_store import ContextId
from nexus.piping.dsl import Transform
from nexus.runtime.actor import ActorBuilder, TransformActor
from nexus.runtime.events import ToBus
from nexus.utils.utils import default_name


class Stringify[T](Transform[T, str], ActorBuilder):
    name: str

    def __init__(self, *,
                 name: str | None = None,
                 ) -> None:
        if name is None:
            name = default_name(self)
        super().__init__(name=name)
        self.name = name

    def build_actor(self, *, to_bus: ToBus):
        return StringifyActor[T](spec=self, to_bus=to_bus)


class StringifyActor[T](TransformActor[T, str]):
    def __init__(self, *, spec: Stringify[T], to_bus: ToBus) -> None:
        super().__init__(name=spec.name, spec=spec, to_bus=to_bus)

    def _transform(self, ctx: ContextId, payload: T) -> str:
        return str(payload)
