from __future__ import annotations

from nexus.utils.env import get_required_env_var
from nexus.utils.exceptions import ActorMisconfiguredException, NexusException, SubnetMisconfiguredException
from nexus.utils.types import NetUid


def validate_netuid(
    netuid: NetUid,
    *,
    field_name: str = "netuid",
    exception_type: type[NexusException] = ActorMisconfiguredException,
) -> NetUid:
    if int(netuid) < 0:
        raise exception_type(f"{field_name} must be >= 0")
    return netuid


def load_required_netuid_from_env(env_var: str = "VALIDATOR_NETUID") -> NetUid:
    raw_netuid = get_required_env_var(env_var)
    try:
        return validate_netuid(NetUid(int(raw_netuid)), field_name=env_var, exception_type=SubnetMisconfiguredException)
    except ValueError as exc:
        raise SubnetMisconfiguredException(f"{env_var} must be an integer") from exc
