import importlib
import importlib.util
from types import TracebackType

import httpx
import pytest
from pydantic import BaseModel, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from nexus.utils import openrouter_client
from nexus.utils.exceptions import SubnetMisconfiguredException
from nexus.utils.openrouter_config import OpenRouterSettingsMixin
from nexus.utils.subnet_settings import (
    get_subnet_settings_as,
    initialize_subnet_settings,
    subnet_settings,
)


def test_subnet_settings_module_exports_subnet_settings_api() -> None:
    spec = importlib.util.find_spec("nexus.utils.subnet_settings")

    assert spec is not None
    module = importlib.import_module("nexus.utils.subnet_settings")
    assert hasattr(module, "subnet_settings")
    assert hasattr(module, "get_subnet_settings_as")
    assert hasattr(module, "initialize_subnet_settings")


def test_openrouter_settings_mixin_reads_validator_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VALIDATOR_OPENROUTER_URL", "https://router.test/api")
    monkeypatch.setenv("VALIDATOR_OPENROUTER_API_KEY", "secret")
    monkeypatch.setenv("VALIDATOR_OPENROUTER_MODEL", "model-x")
    monkeypatch.setenv("VALIDATOR_OPENROUTER_TIMEOUT_SECONDS", "12.5")
    monkeypatch.setenv("VALIDATOR_OPENROUTER_TEMPERATURE", "0.1")

    settings = _TestValidatorSettings()  # pyright: ignore[reportCallIssue]

    assert settings.openrouter_url == "https://router.test/api"
    assert settings.openrouter_api_key == "secret"
    assert settings.openrouter_model == "model-x"
    assert settings.validation_openrouter_timeout_seconds == 12.5
    assert settings.validation_openrouter_temperature == 0.1


def test_get_subnet_settings_as_returns_initialized_settings() -> None:
    settings = _TestValidatorSettings(
        openrouter_url="https://router.test/api",
        openrouter_api_key="secret",
        openrouter_model="model-x",
        validation_openrouter_timeout_seconds=12.5,
        validation_openrouter_temperature=0.1,
    )

    initialize_subnet_settings(settings)

    assert get_subnet_settings_as(OpenRouterSettingsMixin) is settings


def test_get_subnet_settings_as_rejects_missing_settings() -> None:
    with pytest.raises(SubnetMisconfiguredException):
        get_subnet_settings_as(OpenRouterSettingsMixin)


def test_subnet_settings_context_restores_previous_settings() -> None:
    outer = _TestValidatorSettings(
        openrouter_url="https://router.test/api",
        openrouter_api_key="secret",
        openrouter_model="model-x",
        validation_openrouter_timeout_seconds=12.5,
        validation_openrouter_temperature=0.1,
    )
    inner = _TestValidatorSettings(
        openrouter_url="https://router.override/api",
        openrouter_api_key="override-secret",
        openrouter_model="model-y",
        validation_openrouter_timeout_seconds=9.0,
        validation_openrouter_temperature=0.0,
    )
    initialize_subnet_settings(outer)

    with subnet_settings(inner):
        assert get_subnet_settings_as(OpenRouterSettingsMixin) is inner

    assert get_subnet_settings_as(OpenRouterSettingsMixin) is outer


def test_subnet_settings_context_cleans_up_without_previous_settings() -> None:
    settings = _TestValidatorSettings(
        openrouter_url="https://router.test/api",
        openrouter_api_key="secret",
        openrouter_model="model-x",
        validation_openrouter_timeout_seconds=12.5,
        validation_openrouter_temperature=0.1,
    )

    with subnet_settings(settings):
        assert get_subnet_settings_as(OpenRouterSettingsMixin) is settings

    with pytest.raises(SubnetMisconfiguredException):
        get_subnet_settings_as(OpenRouterSettingsMixin)


def test_initialize_subnet_settings_rejects_second_initialization() -> None:
    first = _TestValidatorSettings(
        openrouter_url="https://router.test/api",
        openrouter_api_key="secret",
        openrouter_model="model-x",
        validation_openrouter_timeout_seconds=12.5,
        validation_openrouter_temperature=0.1,
    )
    second = _TestValidatorSettings(
        openrouter_url="https://router.other/api",
        openrouter_api_key="other-secret",
        openrouter_model="model-y",
        validation_openrouter_timeout_seconds=9.0,
        validation_openrouter_temperature=0.0,
    )

    initialize_subnet_settings(first)

    with pytest.raises(RuntimeError, match="already initialized"):
        initialize_subnet_settings(second)


def test_get_subnet_settings_as_rejects_wrong_mixin() -> None:
    with subnet_settings(_NonOpenRouterSettings()):
        with pytest.raises(SubnetMisconfiguredException):
            get_subnet_settings_as(OpenRouterSettingsMixin)


@pytest.mark.parametrize("field_name", ["openrouter_url", "openrouter_api_key", "openrouter_model"])
def test_openrouter_settings_mixin_rejects_blank_string_fields(field_name: str) -> None:
    with pytest.raises(ValidationError, match=field_name):
        if field_name == "openrouter_url":
            _TestValidatorSettings(
                openrouter_url="   ",
                openrouter_api_key="secret",
                openrouter_model="model-x",
                validation_openrouter_timeout_seconds=12.5,
                validation_openrouter_temperature=0.1,
            )
        elif field_name == "openrouter_api_key":
            _TestValidatorSettings(
                openrouter_url="https://router.test/api",
                openrouter_api_key="   ",
                openrouter_model="model-x",
                validation_openrouter_timeout_seconds=12.5,
                validation_openrouter_temperature=0.1,
            )
        else:
            _TestValidatorSettings(
                openrouter_url="https://router.test/api",
                openrouter_api_key="secret",
                openrouter_model="   ",
                validation_openrouter_timeout_seconds=12.5,
                validation_openrouter_temperature=0.1,
            )


@pytest.mark.parametrize(
    "field_name",
    ["validation_openrouter_timeout_seconds", "validation_openrouter_temperature"],
)
def test_openrouter_settings_mixin_rejects_bool_numeric_fields(field_name: str) -> None:
    with pytest.raises(ValidationError, match=field_name):
        if field_name == "validation_openrouter_timeout_seconds":
            _TestValidatorSettings(
                openrouter_url="https://router.test/api",
                openrouter_api_key="secret",
                openrouter_model="model-x",
                validation_openrouter_timeout_seconds=True,
                validation_openrouter_temperature=0.1,
            )
        else:
            _TestValidatorSettings(
                openrouter_url="https://router.test/api",
                openrouter_api_key="secret",
                openrouter_model="model-x",
                validation_openrouter_timeout_seconds=12.5,
                validation_openrouter_temperature=True,
            )


class _QueryResponseModel(BaseModel):
    ok: bool


class _TestValidatorSettings(OpenRouterSettingsMixin, BaseSettings):
    model_config = SettingsConfigDict(env_prefix="VALIDATOR_", extra="ignore")


class _NonOpenRouterSettings(BaseSettings):
    some_other_field: str = "value"


class _FakeOpenRouterResponse:
    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return {"choices": [{"message": {"content": '{"ok": true}'}}]}


def test_openrouter_client_accepts_settings_mixin_and_reads_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class _FakeClient:
        def __init__(self, *, transport: httpx.BaseTransport, timeout: object) -> None:
            captured["transport"] = transport
            captured["timeout"] = timeout

        def __enter__(self) -> _FakeClient:
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> None:
            return None

        def post(self, url: str, *, json: dict[str, object], headers: dict[str, str]) -> _FakeOpenRouterResponse:
            captured["url"] = url
            captured["json"] = json
            captured["headers"] = headers
            return _FakeOpenRouterResponse()

    monkeypatch.setattr(openrouter_client.httpx, "Client", _FakeClient)
    settings = _TestValidatorSettings(
        openrouter_url="https://router.test/api",
        openrouter_api_key="secret",
        openrouter_model="model-x",
        validation_openrouter_timeout_seconds=12.5,
        validation_openrouter_temperature=0.1,
    )
    client = openrouter_client.OpenRouterClient.from_settings(settings)

    response = client.query(
        messages=[{"role": "user", "content": "hello"}],
        response_model=_QueryResponseModel,
    )

    assert response == _QueryResponseModel(ok=True)
    assert captured["timeout"] == 12.5
    assert captured["url"] == "https://router.test/api"
    assert captured["json"] == {
        "model": "model-x",
        "temperature": 0.1,
        "messages": [{"role": "user", "content": "hello"}],
    }
    assert captured["headers"] == {"Authorization": "Bearer secret"}
