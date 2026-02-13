from threading import Thread
from typing import NewType

from nexus.actors.uppercase_or_error import UppercaseOrError

from nexus.actors.stringify import Stringify

from nexus.actors import (
    RestEntryPoint,
)
from nexus.core.dsl.flow import Flow
from nexus.core.dsl.piping import Piping
from pydantic import BaseModel

from nexus.core.runtime.actor import Actor
from nexus.core.runtime.context_store import InMemoryContextStorePersistence, ContextStore
from nexus.core.runtime.event_bus import EventBus
from nexus.core.runtime.events import PipeToBus

S3Url = NewType("S3Url", str)
ImageName = NewType("ImageName", str)


class SingleCatImageInput(BaseModel):
    """
    User request model for the cat-images subnet.

    `image_s3_url` refers to the original background image stored on S3; `image_name` is a file name used in
    constructing upload keys.
    """

    image_s3_url: S3Url
    image_name: ImageName


class Validator:
    # Actors
    entry: RestEntryPoint[SingleCatImageInput]
    stringify: Stringify[SingleCatImageInput]
    mining_task: UppercaseOrError

    stringify_error: Stringify[Exception]

    piping: Piping
    event_bus: EventBus

    def __init__(self, port: int = 8081) -> None:
        self.entry = RestEntryPoint(
            _id="cat-images-user-requests",
            path="/cat-images",
            port=port,
            user_data_model=SingleCatImageInput,
        )

        self.stringify = Stringify("stringify-user-request")
        self.mining_task = UppercaseOrError("simulate-mining-task-that-can-succeed-or-fail")

        self.stringify_error = Stringify("stringify-error")

        subnet_flow: Flow = (
            Flow.from_connectable(self.entry)
            .then(self.stringify)
            .then(self.mining_task)
            .then(
                ok=self.entry,
                error=Flow.from_connectable(self.stringify_error).then(self.entry)
            )
        )

        nodes = [self.entry, self.stringify, self.mining_task, self.stringify_error]

        piping: Piping = Piping()
        for node in nodes:
            piping.add_flow(Flow.from_connectable(node))
        piping.add_flow(subnet_flow)

        persistence: InMemoryContextStorePersistence = InMemoryContextStorePersistence()
        context_store: ContextStore = ContextStore.recover_from(persistence).context_store

        pipe_to_bus = PipeToBus()
        actors: list[Actor] = [node.build_actor(pipe_to_bus=pipe_to_bus, context_store=context_store)
                               for node in nodes]


        self.event_bus = EventBus(piping.pipes, pipe_to_bus, actors, context_store)

    def run_loop(self) -> tuple[Thread, ...]:
        jobs: list[Thread] = []
        for actor in self.event_bus.sinks.values():
            jobs.append(actor.run_loop())

        jobs.append(self.event_bus.run_loop())
        return tuple(jobs)

    def stop(self):
        self.event_bus.request_stop()

