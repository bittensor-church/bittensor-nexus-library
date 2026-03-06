from __future__ import annotations

import os

from nexus.utils.exceptions import InternalFrameworkException


def get_required_env_var(*keys: str) -> str:
    if len(keys) == 0:
        raise InternalFrameworkException("At least one environment variable key must be provided")

    for key in keys:
        value = os.getenv(key)
        if value is not None and len(value) > 0:
            return value

    raise InternalFrameworkException(f"Missing required environment variable; expected one of: {', '.join(keys)}")
