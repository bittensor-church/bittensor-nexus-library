# pyright: basic

from pathlib import Path

import pytest
from pydantic_settings import BaseSettings, SettingsConfigDict
from pylon_client.artanis import AsyncPylonClient

from nexus.v1 import (
    EnvAsyncPylonClientProvider,
    PylonClientSettingsMixin,
    SubnetMisconfiguredException,
    subnet_settings,
)

BASE_ENV = {
    "VALIDATOR_PYLON_SERVICE_ADDRESS": "http://pylon:8000",
    "VALIDATOR_PYLON_OPEN_ACCESS_TOKEN": "test-token",
}


class _PylonOnlySettings(PylonClientSettingsMixin, BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")


def test_raises_when_only_identity_name_set(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in BASE_ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("VALIDATOR_PYLON_IDENTITY_NAME", "validator-01")

    with pytest.raises(SubnetMisconfiguredException, match="VALIDATOR_PYLON_IDENTITY_NAME"):
        _PylonOnlySettings()  # type: ignore[call-arg]


def test_raises_when_only_identity_token_set(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in BASE_ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("VALIDATOR_PYLON_IDENTITY_TOKEN", "secret-token")

    with pytest.raises(SubnetMisconfiguredException, match="VALIDATOR_PYLON_IDENTITY_NAME"):
        _PylonOnlySettings()  # type: ignore[call-arg]


def test_creates_client_with_minimal_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in BASE_ENV.items():
        monkeypatch.setenv(k, v)

    with subnet_settings(_PylonOnlySettings()):  # type: ignore[call-arg]
        client = EnvAsyncPylonClientProvider().get_client()

    assert isinstance(client, AsyncPylonClient)
    assert client.config.address == "http://pylon:8000"
    assert client.config.mtls_cert_path is None
    assert client.config.mtls_key_path is None
    assert client.config.neurons_file is None


def test_passes_cert_and_key_paths_to_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cert_file = tmp_path / "cert.pem"
    key_file = tmp_path / "key.pem"
    cert_file.write_text("cert")
    key_file.write_text("key")

    for k, v in BASE_ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("VALIDATOR_MTLS_CERT_PATH", str(cert_file))
    monkeypatch.setenv("VALIDATOR_MTLS_KEY_PATH", str(key_file))

    with subnet_settings(_PylonOnlySettings()):  # type: ignore[call-arg]
        client = EnvAsyncPylonClientProvider().get_client()

    assert isinstance(client, AsyncPylonClient)
    assert str(client.config.mtls_cert_path) == str(cert_file)
    assert str(client.config.mtls_key_path) == str(key_file)


def test_passes_neurons_file_to_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    neurons_file = tmp_path / "neurons.json"
    neurons_file.write_text("[]")

    for k, v in BASE_ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("VALIDATOR_NEURONS_FILE", str(neurons_file))

    with subnet_settings(_PylonOnlySettings()):  # type: ignore[call-arg]
        client = EnvAsyncPylonClientProvider().get_client()

    assert isinstance(client, AsyncPylonClient)
    assert str(client.config.neurons_file) == str(neurons_file)
