from __future__ import annotations

import os

from nexus._internal.utils.exceptions import SubnetMisconfiguredException


def _get_non_empty_env_var(key: str) -> str | None:
    value = os.getenv(key)
    if value is None or len(value) == 0:
        return None
    return value


def get_optional_env_var(key: str) -> str | None:
    return _get_non_empty_env_var(key)


def get_required_env_var(key: str) -> str:
    value = _get_non_empty_env_var(key)
    if value is not None:
        return value
    raise SubnetMisconfiguredException(f"Missing required environment variable: {key}")
