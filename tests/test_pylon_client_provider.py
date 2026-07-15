# pyright: basic

import pytest

from nexus.v1 import EnvAsyncPylonClientProvider, SubnetMisconfiguredException

BASE_ENV = {
    "VALIDATOR_PYLON_SERVICE_ADDRESS": "http://pylon:8000",
    "VALIDATOR_PYLON_OPEN_ACCESS_TOKEN": "test-token",
}


def test_raises_when_only_identity_name_set(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in BASE_ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("VALIDATOR_PYLON_IDENTITY_NAME", "validator-01")

    with pytest.raises(SubnetMisconfiguredException, match="VALIDATOR_PYLON_IDENTITY_NAME"):
        EnvAsyncPylonClientProvider().get_client()


def test_raises_when_only_identity_token_set(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in BASE_ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("VALIDATOR_PYLON_IDENTITY_TOKEN", "secret-token")

    with pytest.raises(SubnetMisconfiguredException, match="VALIDATOR_PYLON_IDENTITY_NAME"):
        EnvAsyncPylonClientProvider().get_client()


def test_creates_client_with_minimal_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in BASE_ENV.items():
        monkeypatch.setenv(k, v)

    client = EnvAsyncPylonClientProvider().get_client()

    assert client.config.address == "http://pylon:8000"
    assert client.config.mtls_cert_path is None
    assert client.config.mtls_key_path is None
    assert client.config.neurons_file is None


def test_passes_cert_and_key_paths_to_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pytest.TempdirFactory,
) -> None:
    cert_file = tmp_path / "cert.pem"  # type: ignore[operator]
    key_file = tmp_path / "key.pem"  # type: ignore[operator]
    cert_file.write_text("cert")  # type: ignore[union-attr]
    key_file.write_text("key")  # type: ignore[union-attr]

    for k, v in BASE_ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("VALIDATOR_MTLS_CERT_PATH", str(cert_file))
    monkeypatch.setenv("VALIDATOR_MTLS_KEY_PATH", str(key_file))

    client = EnvAsyncPylonClientProvider().get_client()

    assert str(client.config.mtls_cert_path) == str(cert_file)
    assert str(client.config.mtls_key_path) == str(key_file)


def test_passes_neurons_file_to_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pytest.TempdirFactory,
) -> None:
    neurons_file = tmp_path / "neurons.json"  # type: ignore[operator]
    neurons_file.write_text("[]")  # type: ignore[union-attr]

    for k, v in BASE_ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("VALIDATOR_NEURONS_FILE", str(neurons_file))

    client = EnvAsyncPylonClientProvider().get_client()

    assert str(client.config.neurons_file) == str(neurons_file)
