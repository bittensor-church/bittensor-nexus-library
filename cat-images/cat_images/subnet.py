from typing import NewType

from nexus.actors.payload_creator import WithPresignedUrl
from pydantic import BaseModel

S3Url = NewType("S3Url", str)
ImageName = NewType("ImageName", str)
ImageHash = NewType("ImageHash", str)


class SingleCatImageInput(BaseModel):
    """
    User request model for the cat-images subnet.

    `image_s3_url` refers to the original background image stored on S3; `image_name` is a file name used in
    constructing upload keys.
    """

    image_s3_url: S3Url
    image_name: ImageName


type MinerPayload = WithPresignedUrl[SingleCatImageInput]
MinerPayloadModel = MinerPayload.__value__


class MinerResult(BaseModel):
    image_hash: ImageHash


type MinerPublicResult = WithPresignedUrl[MinerResult]


class ValidationResult(BaseModel):
    score: int
