"""Actor-facing provider for constructing OpenRouter clients."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Final, override

from nexus.utils.openrouter_client import OpenRouterClient
from nexus.utils.openrouter_config import OpenRouterSettingsMixin
from nexus.utils.subnet_settings import get_subnet_settings_as


class OpenRouterClientProvider(ABC):
    """Builds OpenRouter clients for actors that need local inference access."""

    @abstractmethod
    def get_client(self) -> OpenRouterClient: ...


class SubnetSettingsOpenRouterClientProvider(OpenRouterClientProvider):
    """Build an OpenRouter client from the subnet settings currently scoped to the runtime."""

    @override
    def get_client(self) -> OpenRouterClient:
        settings = get_subnet_settings_as(OpenRouterSettingsMixin)
        return OpenRouterClient.from_settings(settings)


DEFAULT_OPENROUTER_CLIENT_PROVIDER: Final[OpenRouterClientProvider] = SubnetSettingsOpenRouterClientProvider()
