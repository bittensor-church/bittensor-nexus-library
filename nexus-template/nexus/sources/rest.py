from nexus.piping.dsl import Source
from nexus.runtime.actor import Actor, ActorBuilder
from nexus.runtime.event_bus import ToBus
from nexus.utils.utils import default_name


class RestEntryPoint[Model](Source[Model], ActorBuilder):
    name: str
    __path: str
    __port: int
    __user_data_model: Model

    def __init__(self, *,
                 name: str | None = None,
                 path: str,
                 port: int,
                 user_data_model: Model
                 ) -> None:
        if name is None:
            name = default_name(self)
        super().__init__(self, source_id=name)
        self.name = name
        self.__path = path
        self.__port = port
        self.__user_data_model = user_data_model

    def build_actor(self, *, to_bus: ToBus) -> Actor:
        pass
