from collections.abc import Generator
from contextlib import contextmanager
from typing import NewType

from nexus.actors import (
    RestEntryPoint,
)
from nexus.actors.stringify import Stringify
from nexus.actors.uppercase_or_error import UppercaseOrError
from nexus.core.dsl.flow import Flow
from nexus.core.dsl.piping import Piping
from nexus.core.runtime.event_bus import EventBus
from nexus.core.runtime.subnet_runtime import SubnetBuilder, SubnetRuntime
from pydantic import BaseModel

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

    runtime: SubnetRuntime

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

        self.runtime = SubnetBuilder(nodes=nodes).add_flows(subnet_flow).build()

    @contextmanager
    def running(self, shutdown_timeout_seconds: float = 30.0) -> Generator[SubnetRuntime]:
        with self.runtime.running(shutdown_timeout_seconds=shutdown_timeout_seconds) as runtime:
            yield runtime
