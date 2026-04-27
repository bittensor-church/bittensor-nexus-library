# pyright: reportUnusedImport=false, reportUnsupportedDunderAll=false
"""Public v1 interface of the Nexus package."""

from nexus._internal.actors import pylon_client_provider as pylon_client_provider
from nexus._internal.logging_utils import configure_default_logging as configure_default_logging
from nexus._internal.logging_utils import get_logger as get_logger
from nexus._internal.nexus_validator import NexusValidator as NexusValidator
from nexus._internal.utils import openrouter_client as openrouter_client
from nexus._internal.utils import subnet_settings as subnet_settings_module

from .actors import *
from .actors import __all__ as _actors_all
from .core import *
from .core import __all__ as _core_all
from .utils import *
from .utils import __all__ as _utils_all

__all__ = sorted(
    {
        *_actors_all,
        *_core_all,
        *_utils_all,
        "NexusValidator",
        "configure_default_logging",
        "get_logger",
        "openrouter_client",
        "subnet_settings_module",
    }
)


def __dir__() -> list[str]:
    return __all__
