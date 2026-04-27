from pydantic import BaseModel, ConfigDict, Field, field_validator


class OpenRouterSettingsMixin(BaseModel):
    """Shared validator settings fields required for OpenRouter chat-completions calls."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    openrouter_url: str = Field(validation_alias="VALIDATOR_OPENROUTER_URL")
    openrouter_api_key: str = Field(validation_alias="VALIDATOR_OPENROUTER_API_KEY")
    openrouter_model: str = Field(validation_alias="VALIDATOR_OPENROUTER_MODEL")
    validation_openrouter_timeout_seconds: float = Field(validation_alias="VALIDATOR_OPENROUTER_TIMEOUT_SECONDS")
    validation_openrouter_temperature: float = Field(validation_alias="VALIDATOR_OPENROUTER_TEMPERATURE")

    @field_validator("openrouter_url", "openrouter_api_key", "openrouter_model")
    @classmethod
    def _require_non_blank_strings(cls, value: str) -> str:
        if len(value.strip()) == 0:
            raise ValueError("must be a non-empty string")
        return value

    @field_validator("validation_openrouter_timeout_seconds", "validation_openrouter_temperature", mode="before")
    @classmethod
    def _reject_bool_numeric_values(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("must be numeric, got bool")
        return value
