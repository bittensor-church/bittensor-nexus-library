from typing import NewType

from nexus.actors import (
    RestEntryPoint,
    Stringify,
    UppercaseOrError,
)
from nexus.core.dsl.piping import Piping
from pydantic import BaseModel

S3Url = NewType("S3Url", str)

ImageName = NewType("ImageName", str)

class SingleSubnetPocInput(BaseModel):
    """
    User request model for the subnet PoC.

    `image_s3_url` refers to the original
    background image stored on S3; `image_name` is a file name used in
    constructing upload keys.
    """

    # this is essentially user data
    # apart from that we have some additional data kept in the context like
    # user_id, request_id
    image_s3_url: S3Url
    image_name: ImageName


def make_subnet():
    entry: RestEntryPoint[SingleSubnetPocInput] = RestEntryPoint(
        path="/cat",
        port=8080,
        user_data_model=SingleSubnetPocInput)

    stringify: Stringify[SingleSubnetPocInput] = Stringify()

    mining_task: UppercaseOrError = UppercaseOrError()

    stringify_error: Stringify[Exception] = Stringify()

    piping = Piping()
    piping.connect(entry.source, stringify.sink)
    piping.connect(stringify.ok, mining_task.sink)
    piping.connect(mining_task.ok, entry.sink)
    piping.connect(mining_task.error, stringify_error.sink)
    piping.connect(stringify_error.ok, entry.sink)

    return piping, [entry, stringify, mining_task, stringify_error]
