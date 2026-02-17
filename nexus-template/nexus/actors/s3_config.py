from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class S3Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="NEXUS_S3_",
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
    )

    bucket_name: str
    aws_access_key_id: str
    aws_secret_access_key: str
    region_name: str
    endpoint_url: str | None = None
    s3_addressing_style: Literal["path", "virtual"] = Field(
        default="path",
        validation_alias="NEXUS_S3_ADDRESSING_STYLE",
    )
