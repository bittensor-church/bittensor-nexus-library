# pyright: basic

import pytest

from nexus.utils.exceptions import ActorMisconfiguredException, SubnetMisconfiguredException
from nexus.utils.netuid import load_required_netuid_from_env, validate_netuid
from nexus.utils.types import NetUid


def test_validate_netuid_accepts_non_negative_value() -> None:
    assert validate_netuid(NetUid(11)) == NetUid(11)


def test_validate_netuid_rejects_negative_value() -> None:
    with pytest.raises(ActorMisconfiguredException, match="netuid must be >= 0"):
        validate_netuid(NetUid(-1))


def test_load_required_netuid_from_env_reads_and_validates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VALIDATOR_NETUID", "17")

    assert load_required_netuid_from_env() == NetUid(17)


def test_load_required_netuid_from_env_rejects_negative_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VALIDATOR_NETUID", "-1")

    with pytest.raises(SubnetMisconfiguredException, match="VALIDATOR_NETUID must be >= 0"):
        load_required_netuid_from_env()


def test_load_required_netuid_from_env_rejects_non_integer_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VALIDATOR_NETUID", "abc")

    with pytest.raises(SubnetMisconfiguredException, match="VALIDATOR_NETUID must be an integer"):
        load_required_netuid_from_env()
