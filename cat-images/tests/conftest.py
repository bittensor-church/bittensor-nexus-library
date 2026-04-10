import nexus.utils.subnet_settings as subnet_settings_module
import pytest


@pytest.fixture(autouse=True)
def _isolate_subnet_settings_between_tests(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        subnet_settings_module,
        "_subnet_settings_registry",
        subnet_settings_module._SubnetSettingsRegistry(),
    )
