from dataclasses import dataclass

from core.dsl import flow
from actors.rest import RestEntryPoint
from nexus.actors.stringify import Stringify
from nexus.actors.uppercase_or_error import UppercaseOrError
from pydantic import BaseModel


@dataclass
class S3Url(str):
    """
    URL of an image stored on S3.
    """


@dataclass
class ImageName(str):
    """
    Name of an image file.
    """


class SingleCatImageInput(BaseModel):
    """
    User request model for generating a cat‑augmented image.

    `image_s3_url` refers to the original
    background image stored on S3; `image_name` is a file name used in
    constructing upload keys.
    """

    # this is essentially user data
    # apart from that we have some additional data kept in the context like
    # user_id, request_id
    image_s3_url: S3Url
    image_name: ImageName


class CatImagesValidator:
    """
    Definition of the cat‑images subnet validator.  It wires together generic Nexus
    components with domain‑specific data models and helper functions.
    """

    entry: RestEntryPoint[SingleCatImageInput] = RestEntryPoint(
        path="/cat",
        port=8080,
        user_data_model=SingleCatImageInput)

    stringify: Stringify[SingleCatImageInput] = Stringify()

    forking_task: UppercaseOrError = UppercaseOrError()


    flow: Flow = flow(entry)
         .then(stringify)
         .then(forking_task)
         .then(ok=stringify
                    .then(stringify),
               error=stringify)

    # or maybe ?
    # flow: Flow = Flow.start(entry.output)
    #                   .then(stringify)
    #                   .then(entry.input)
