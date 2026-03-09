# pyright: basic

from typing import cast

import pytest

from nexus.actors import pylon_client_provider
from nexus.utils.exceptions import ActorMisconfiguredException


class _FakePylonClient:
    def __init__(self, config):
        self.config = config


def _set_required_provider_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VALIDATOR_PYLON_SERVICE_ADDRESS", raising=False)
    monkeypatch.delenv("VALIDATOR_PYLON_OPEN_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("VALIDATOR_PYLON_IDENTITY_NAME", raising=False)
    monkeypatch.delenv("VALIDATOR_PYLON_IDENTITY_TOKEN", raising=False)
    monkeypatch.setenv("VALIDATOR_PYLON_SERVICE_ADDRESS", "http://pylon:8000")
    monkeypatch.setenv("VALIDATOR_PYLON_OPEN_ACCESS_TOKEN", "open-access-token")


def test_provider_binds_open_access_only_when_identity_not_set(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_provider_env(monkeypatch)
    monkeypatch.setattr(pylon_client_provider, "PylonClient", _FakePylonClient)

    provider = pylon_client_provider.EnvPylonClientProvider()
    client = cast(_FakePylonClient, provider.get_client())
    config = client.config

    assert config.address == "http://pylon:8000"
    assert str(config.open_access_token) == "open-access-token"
    assert config.identity_name is None
    assert config.identity_token is None


def test_provider_binds_identity_from_validator_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_provider_env(monkeypatch)
    monkeypatch.setenv("VALIDATOR_PYLON_IDENTITY_NAME", "validator")
    monkeypatch.setenv("VALIDATOR_PYLON_IDENTITY_TOKEN", "identity-token")
    monkeypatch.setattr(pylon_client_provider, "PylonClient", _FakePylonClient)

    provider = pylon_client_provider.EnvPylonClientProvider()
    client = cast(_FakePylonClient, provider.get_client())
    config = client.config

    assert config.identity_name == "validator"
    assert str(config.identity_token) == "identity-token"


def test_provider_rejects_partial_identity_name_only(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_provider_env(monkeypatch)
    monkeypatch.setenv("VALIDATOR_PYLON_IDENTITY_NAME", "validator")
    monkeypatch.delenv("VALIDATOR_PYLON_IDENTITY_TOKEN", raising=False)

    provider = pylon_client_provider.EnvPylonClientProvider()

    with pytest.raises(ActorMisconfiguredException, match="both name and token"):
        provider.get_client()


def test_provider_rejects_partial_identity_token_only(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_provider_env(monkeypatch)
    monkeypatch.delenv("VALIDATOR_PYLON_IDENTITY_NAME", raising=False)
    monkeypatch.setenv("VALIDATOR_PYLON_IDENTITY_TOKEN", "identity-token")

    provider = pylon_client_provider.EnvPylonClientProvider()

    with pytest.raises(ActorMisconfiguredException, match="both name and token"):
        provider.get_client()


def test_provider_rejects_missing_required_validator_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_provider_env(monkeypatch)
    monkeypatch.delenv("VALIDATOR_PYLON_OPEN_ACCESS_TOKEN", raising=False)

    provider = pylon_client_provider.EnvPylonClientProvider()

    with pytest.raises(ActorMisconfiguredException, match="VALIDATOR_PYLON_OPEN_ACCESS_TOKEN"):
        provider.get_client()
