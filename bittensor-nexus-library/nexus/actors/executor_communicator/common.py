from datetime import timedelta
from ipaddress import IPv4Address, IPv6Address
from typing import NewType

from nexus.utils.exceptions import ActorMisconfiguredException

NormalizedHttpPath = NewType("NormalizedHttpPath", str)
UrlHost = NewType("UrlHost", str)


def normalize_http_path(path: str) -> NormalizedHttpPath:
    normalized = path.strip()
    if normalized == "":
        raise ActorMisconfiguredException("HTTP path must not be empty.")
    return NormalizedHttpPath(normalized if normalized.startswith("/") else f"/{normalized}")


def format_host_for_url(host: str | IPv4Address | IPv6Address) -> UrlHost:
    stripped = str(host).strip()
    if stripped == "":
        raise ActorMisconfiguredException("HTTP host must not be empty.")
    if ":" in stripped and not (stripped.startswith("[") and stripped.endswith("]")):
        return UrlHost(f"[{stripped}]")
    return UrlHost(stripped)


def validate_positive_timeout(*, timeout: timedelta, parameter_name: str) -> None:
    if timeout <= timedelta(0):
        raise ActorMisconfiguredException(f"{parameter_name} must be > 0.")


def timeout_seconds(timeout: timedelta) -> float:
    return timeout.total_seconds()
