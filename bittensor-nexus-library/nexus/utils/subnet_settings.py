from collections.abc import Generator
from contextlib import contextmanager
from typing import cast

from pydantic_settings import BaseSettings

from nexus.utils.exceptions import SubnetMisconfiguredException


class _SubnetSettingsRegistry:
    def __init__(self) -> None:
        self._subnet_settings: BaseSettings | None = None

    def initialize(self, settings: BaseSettings) -> None:
        """Register the process-wide subnet settings object once.

        Raises:
            RuntimeError: If subnet settings were already initialized.
        """
        if self._subnet_settings is not None:
            raise RuntimeError("Subnet settings are already initialized.")
        self._subnet_settings = settings

    def get_as[SettingsT](self, required_mixin: type[SettingsT]) -> SettingsT:
        """Return the registered subnet settings object if it implements the requested mixin.

        Raises:
            SubnetMisconfiguredException: If no subnet settings are registered or the mixin is not implemented.
        """
        settings = self._subnet_settings
        if settings is None:
            raise SubnetMisconfiguredException("Subnet settings are not registered.")
        if not isinstance(settings, required_mixin):
            raise SubnetMisconfiguredException(
                f"Subnet settings {type(settings).__name__} do not implement {required_mixin.__name__}."
            )
        return cast(SettingsT, settings)

    @contextmanager
    def use(self, settings: BaseSettings) -> Generator[BaseSettings]:
        """Temporarily replace the registered subnet settings within a context."""
        previous_settings = self._subnet_settings
        self._subnet_settings = settings
        try:
            yield settings
        finally:
            self._subnet_settings = previous_settings


_subnet_settings_registry = _SubnetSettingsRegistry()


def initialize_subnet_settings(settings: BaseSettings) -> None:
    """Register the process-wide subnet settings object once."""
    _subnet_settings_registry.initialize(settings)


def get_subnet_settings_as[SettingsT](required_mixin: type[SettingsT]) -> SettingsT:
    """Return the registered subnet settings object if it implements the requested mixin."""
    return _subnet_settings_registry.get_as(required_mixin)


@contextmanager
def subnet_settings(settings: BaseSettings) -> Generator[BaseSettings]:
    """Temporarily replace the registered subnet settings within a context."""
    with _subnet_settings_registry.use(settings) as scoped_settings:
        yield scoped_settings
