from typing import override

from nexus.core.dsl.nodes import HasGlobalId, Sink, Source
from nexus.core.runtime.actor import Actor, ActorBuilder
from nexus.core.runtime.events import PipeToBus


class RestEntryPoint[Model](HasGlobalId, ActorBuilder):
    gid: str
    source: Source[Model]
    sink: Sink[str]
    __path: str
    __port: int
    __user_data_model: type[Model]

    def __init__(self, *,
                 gid_prefix: str | None = None,
                 path: str,
                 port: int,
                 user_data_model: type[Model]
                 ) -> None:
        super().__init__(gid_prefix=gid_prefix)
        self.__path = path
        self.__port = port
        self.__user_data_model = user_data_model
        self.source = Source(gid_prefix)
        self.sink = Sink(gid_prefix)

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus) -> Actor:
        raise NotImplementedError("REST entry point actor is not implemented yet.")
