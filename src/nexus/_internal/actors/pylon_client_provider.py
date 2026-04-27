from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Protocol, override

from pylon_client.artanis import Config, Hotkey, IdentityName, NetUid, PylonAuthToken, PylonClient, Weight
from pylon_client.artanis.v1 import GetLatestBlockInfoResponse, GetNeuronsResponse, SetWeightsResponse

from nexus._internal.utils.env import get_optional_env_var, get_required_env_var
from nexus._internal.utils.exceptions import SubnetMisconfiguredException


class OpenAccessPylonApiLike(Protocol):
    def get_recent_neurons(self, netuid: NetUid) -> GetNeuronsResponse: ...

    def get_latest_block_info(self) -> GetLatestBlockInfoResponse: ...


class IdentityPylonApiLike(Protocol):
    def put_weights(self, weights: dict[Hotkey, Weight]) -> SetWeightsResponse: ...


class SyncPylonClientLike(Protocol):
    """
    Protocol for sync Pylon client. Only includes the parts of the client that are used by actors.

    this is a workaround to avoid importing the actual PylonClient in actors,
    which would make testing harder and create unnecessary dependencies.
    This should be removed once pylon client provides a full-fledged mock
    to be used in contexts like this.
    """

    @property
    def open_access(self) -> OpenAccessPylonApiLike: ...

    @property
    def identity(self) -> IdentityPylonApiLike: ...

    def __enter__(self) -> SyncPylonClientLike: ...

    def __exit__(
        self,
        exc_type: object,
        exc_val: object,
        exc_tb: object,
    ) -> object: ...


class PylonClientProvider(ABC):
    @abstractmethod
    def get_client(self) -> SyncPylonClientLike: ...


class EnvPylonClientProvider(PylonClientProvider):
    @override
    def get_client(self) -> SyncPylonClientLike:
        address = get_required_env_var("VALIDATOR_PYLON_SERVICE_ADDRESS")
        open_access_token = get_required_env_var("VALIDATOR_PYLON_OPEN_ACCESS_TOKEN")
        identity_name = get_optional_env_var("VALIDATOR_PYLON_IDENTITY_NAME")
        identity_token = get_optional_env_var("VALIDATOR_PYLON_IDENTITY_TOKEN")

        if (identity_name is None) != (identity_token is None):
            raise SubnetMisconfiguredException(
                "Pylon identity configuration must provide both name and token. "
                "Expected VALIDATOR_PYLON_IDENTITY_NAME together with VALIDATOR_PYLON_IDENTITY_TOKEN."
            )

        return PylonClient(
            Config(
                address=address,
                open_access_token=PylonAuthToken(open_access_token),
                identity_name=IdentityName(identity_name) if identity_name is not None else None,
                identity_token=PylonAuthToken(identity_token) if identity_token is not None else None,
            )
        )


DEFAULT_PYLON_CLIENT_PROVIDER: PylonClientProvider = EnvPylonClientProvider()
