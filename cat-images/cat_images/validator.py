from typing import NewType

from nexus.actors.uppercase_or_error import UppercaseOrError

from nexus.actors.stringify import Stringify

from nexus.actors import (
    RestEntryPoint,
)
from nexus.core.dsl.flow import Flow
from nexus.core.dsl.piping import Piping
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


def make_subnet():
    entry: RestEntryPoint[SingleCatImageInput] = RestEntryPoint(
        _id="cat-images-user-requests",
        path="/cat-images",
        port=8081,
        user_data_model=SingleCatImageInput,
    )

    stringify: Stringify[SingleCatImageInput] = Stringify("stringify-user-request")
    mining_task: UppercaseOrError = UppercaseOrError("simulate-mining-task-that-can-succeed-or-fail")

    stringify_error: Stringify[Exception] = Stringify("stringify-error")

    subnet_flow: Flow = (
        Flow.from_connectable(entry)
        .then(stringify)
        .then(mining_task)
        .then(
            ok=entry,
            error=Flow.from_connectable(stringify_error).then(entry)
        )
    )

    piping: Piping = Piping()
    piping.add_flow(subnet_flow)

    return piping, [entry, stringify, mining_task, stringify_error]

