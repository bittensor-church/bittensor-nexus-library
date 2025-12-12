from typing import override

from nexus.piping.dsl import Source
from nexus.runtime.actor import Actor, ActorBuilder
from nexus.runtime.events import PipeToBus


class RestEntryPoint[Model](Source[Model], ActorBuilder):
    name: str
    __path: str
    __port: int
    __user_data_model: type[Model]

    def __init__(self, *,
                 name: str | None = None,
                 path: str,
                 port: int,
                 user_data_model: type[Model]
                 ) -> None:
        Source[Model].__init__(self, name)
        ActorBuilder.__init__(self)
        self.__path = path
        self.__port = port
        self.__user_data_model = user_data_model

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus) -> Actor:
        raise NotImplementedError("REST entry point actor is not implemented yet.")
