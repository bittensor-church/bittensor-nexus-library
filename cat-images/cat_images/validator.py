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
        path="/cat-images",
        port=8081,
        user_data_model=SingleCatImageInput,
    )

    stringify: Stringify[SingleCatImageInput] = Stringify()
    mining_task: UppercaseOrError = UppercaseOrError()

    stringify_error: Stringify[Exception] = Stringify()

    piping = Piping()
    piping.connect(entry.source, stringify.sink)
    piping.connect(stringify.ok, mining_task.sink)
    piping.connect(mining_task.ok, entry.sink)
    piping.connect(mining_task.error, stringify_error.sink)
    piping.connect(stringify_error.ok, entry.sink)

    return piping, [entry, stringify, mining_task, stringify_error]

