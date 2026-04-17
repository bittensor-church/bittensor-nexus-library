from typing import Annotated, NewType

from nexus.actors.payload_creator import WithPresignedUrl
from pydantic import AliasChoices, AliasPath, BaseModel, Field

S3Url = NewType("S3Url", str)
ImageHash = NewType("ImageHash", str)


class UserImageInput(BaseModel):
    """User request model for the cat-images subnet.

    `image_s3_url` refers to the original image stored on S3."""

    image_s3_url: S3Url


MinerPayload = WithPresignedUrl[UserImageInput]


class MinerResult(BaseModel):
    image_hash: ImageHash


MinerPublicResult = WithPresignedUrl[MinerResult]


class TaskScores(BaseModel):
    """Structured OpenRouter scoring response keyed by mining task-result id."""

    scores_by_task_result_id: dict[str, Annotated[int, Field(ge=1, le=100)]]


class ValidationResult(BaseModel):
    score: int = Field(ge=1, le=100)


class ValidatorResult(BaseModel):
    """User-facing result delivered by the validator to the facilitator."""

    result_image_url: S3Url = Field(validation_alias=AliasChoices("result_image_url", "presigned_url"))
    image_hash: ImageHash = Field(validation_alias=AliasChoices(AliasPath("input", "image_hash"), "image_hash"))
