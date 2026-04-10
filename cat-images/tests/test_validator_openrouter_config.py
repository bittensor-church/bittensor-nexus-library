import pytest
from nexus.actors.executor_communicator.openrouter_inference_communicator import (
    OpenRouterInferenceCommunicator,
)
from nexus.utils.exceptions import SubnetMisconfiguredException
from nexus.utils.openrouter_config import OpenRouterSettingsMixin
from nexus.utils.subnet_settings import get_subnet_settings_as, subnet_settings

from cat_images.validator import CatValidatorSettings, Validator


def _build_settings() -> CatValidatorSettings:
    return CatValidatorSettings(
        netuid=1,
        openrouter_api_key="settings-api-key",
        openrouter_url="https://settings.test/api",
        openrouter_model="settings-model",
        validation_openrouter_timeout_seconds=33.0,
        validation_openrouter_temperature=0.25,
        external_ip="127.0.0.1",
        pylon_service_address="https://pylon.test",
        pylon_open_access_token="token",
    )


def test_cat_validator_settings_restores_openrouter_defaults() -> None:
    settings = CatValidatorSettings(
        netuid=1,
        openrouter_api_key="settings-api-key",
        external_ip="127.0.0.1",
        pylon_service_address="https://pylon.test",
        pylon_open_access_token="token",
    )

    assert settings.openrouter_url == "https://openrouter.ai/api/v1/chat/completions"
    assert settings.openrouter_model == "google/gemini-2.5-flash-image"
    assert settings.validation_openrouter_timeout_seconds == 120.0
    assert settings.validation_openrouter_temperature == 0.0


def test_validator_validation_task_uses_explicit_subnet_settings_scope_for_openrouter_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for env_var in (
        "VALIDATOR_OPENROUTER_URL",
        "VALIDATOR_OPENROUTER_API_KEY",
        "VALIDATOR_OPENROUTER_MODEL",
        "VALIDATOR_OPENROUTER_TIMEOUT_SECONDS",
        "VALIDATOR_OPENROUTER_TEMPERATURE",
    ):
        monkeypatch.delenv(env_var, raising=False)

    validator_settings = _build_settings()
    validator = Validator(validator_settings)

    communicator = validator.validation_task.executor_communicator
    assert isinstance(communicator, OpenRouterInferenceCommunicator)

    with subnet_settings(validator_settings):
        registered_settings = get_subnet_settings_as(OpenRouterSettingsMixin)

        assert registered_settings is validator_settings
        assert registered_settings.openrouter_url == "https://settings.test/api"
        assert registered_settings.openrouter_api_key == "settings-api-key"
        assert registered_settings.openrouter_model == "settings-model"
        assert registered_settings.validation_openrouter_timeout_seconds == 33.0
        assert registered_settings.validation_openrouter_temperature == 0.25

    with pytest.raises(SubnetMisconfiguredException):
        get_subnet_settings_as(OpenRouterSettingsMixin)
