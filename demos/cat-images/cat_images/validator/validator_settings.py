from typing import Self

from nexus.v1 import NetUid, OpenRouterSettingsMixin, Port, PylonClientSettingsMixin
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_OPENROUTER_MODEL = "google/gemini-2.5-flash-image"
DEFAULT_S3_BUCKET = "my-cat-images-bucket"
DEFAULT_INGRESS_PORT = Port(8081)
DEFAULT_MINER_CALLBACK_PORT = Port(9091)
DEFAULT_VALIDATION_OPENROUTER_TIMEOUT_SECONDS = 120.0
DEFAULT_VALIDATION_OPENROUTER_TEMPERATURE = 0.0
DEFAULT_VALIDATION_PROMPT = (
    "You have a list of image pairs. For each pair determine if the second image looks like the first image, "
    "but with a cat added in a natural, scene-fitting way. Remembed that the cat must be added. If there "
    "are already cats in the original image the processed image should have one more cat added. "
    "Score each pair from 1 to 100, where 1 means the second image does not look like the first with a naturally "
    "added cat, and 100 means it is an excellent natural cat addition. If not cat was added the score should be 1"
    "Return only valid JSON in this exact format: "
    '{"scores_by_task_result_id": {"<task_result_id>": <integer_score_1_to_100>}}. '
    "Do not include markdown, comments, code fences, or any extra keys."
)


class CatValidatorSettings(OpenRouterSettingsMixin, PylonClientSettingsMixin, BaseSettings):
    model_config = SettingsConfigDict(env_prefix="VALIDATOR_", env_file=".env", extra="ignore")

    rest_entry_point_port: Port = DEFAULT_INGRESS_PORT
    miner_callback_port: Port = DEFAULT_MINER_CALLBACK_PORT
    openrouter_url: str = Field(
        default=DEFAULT_OPENROUTER_URL,
        validation_alias="VALIDATOR_OPENROUTER_URL",
    )
    openrouter_model: str = Field(
        default=DEFAULT_OPENROUTER_MODEL,
        validation_alias="VALIDATOR_OPENROUTER_MODEL",
    )
    validation_openrouter_timeout_seconds: float = Field(
        default=DEFAULT_VALIDATION_OPENROUTER_TIMEOUT_SECONDS,
        validation_alias="VALIDATOR_OPENROUTER_TIMEOUT_SECONDS",
    )
    validation_openrouter_temperature: float = Field(
        default=DEFAULT_VALIDATION_OPENROUTER_TEMPERATURE,
        validation_alias="VALIDATOR_OPENROUTER_TEMPERATURE",
    )

    validation_prompt: str = DEFAULT_VALIDATION_PROMPT

    netuid: NetUid
    external_ip: str
    s3_bucket: str = DEFAULT_S3_BUCKET

    @model_validator(mode="after")
    def _normalize_validation_prompt(self) -> Self:
        if len(self.validation_prompt.strip()) == 0:
            self.validation_prompt = DEFAULT_VALIDATION_PROMPT
        return self
