import pytest
from nexus.v1 import subnet_settings_module


@pytest.fixture(autouse=True)
def _isolate_subnet_settings_between_tests(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        subnet_settings_module,
        "_subnet_settings_registry",
        subnet_settings_module._SubnetSettingsRegistry(),
    )
