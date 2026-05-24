from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Protocol, override

from pylon_client.artanis import (
    AsyncConfig,
    AsyncPylonClient,
    BlockNumber,
    Config,
    Hotkey,
    IdentityName,
    MechanismId,
    NetUid,
    PylonAuthToken,
    PylonClient,
    Weight,
)
from pylon_client.artanis.unstable import GetWeightsStatusResponse
from pylon_client.artanis.v1 import GetLatestBlockInfoResponse, GetNeuronsResponse, SetWeightsResponse

from nexus._internal.utils.env import get_optional_env_var, get_required_env_var
from nexus._internal.utils.exceptions import SubnetMisconfiguredException


class OpenAccessPylonApiLike(Protocol):
    def get_recent_neurons(self, netuid: NetUid) -> GetNeuronsResponse: ...

    def get_latest_block_info(self) -> GetLatestBlockInfoResponse: ...


class IdentityPylonApiLike(Protocol):
    def put_weights(self, weights: dict[Hotkey, Weight]) -> SetWeightsResponse: ...


class UnstableIdentityPylonApiLike(Protocol):
    def get_weights_status(
        self,
        block_number: BlockNumber,
        mechanism_id: MechanismId = MechanismId(0),  # noqa: B008
    ) -> GetWeightsStatusResponse: ...


class UnstablePylonNamespaceLike(Protocol):
    @property
    def identity(self) -> UnstableIdentityPylonApiLike: ...


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

    @property
    def unstable(self) -> UnstablePylonNamespaceLike: ...

    def __enter__(self) -> SyncPylonClientLike: ...

    def __exit__(
        self,
        exc_type: object,
        exc_val: object,
        exc_tb: object,
    ) -> object: ...


class PylonClientProvider(ABC):
    """Provides sync Pylon clients to actors that read or write chain state."""

    @abstractmethod
    def get_client(self) -> SyncPylonClientLike: ...


class EnvPylonClientProvider(PylonClientProvider):
    """Builds Pylon clients from validator environment variables."""

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


class AsyncPylonClientProvider(ABC):
    """Provides async Pylon clients to actors that query miners or read chain state."""

    @abstractmethod
    def get_client(self) -> AsyncPylonClient: ...


class EnvAsyncPylonClientProvider(AsyncPylonClientProvider):
    """
    Builds async Pylon clients from validator environment variables.

    Set ``VALIDATOR_MTLS_CERT_PATH`` and ``VALIDATOR_MTLS_KEY_PATH`` to enable
    mTLS when querying neurons via the `get_neuron_client` method. When absent, falls back to plain HTTP.
    """

    @override
    def get_client(self) -> AsyncPylonClient:
        address = get_required_env_var("VALIDATOR_PYLON_SERVICE_ADDRESS")
        open_access_token = get_required_env_var("VALIDATOR_PYLON_OPEN_ACCESS_TOKEN")
        identity_name = get_optional_env_var("VALIDATOR_PYLON_IDENTITY_NAME")
        identity_token = get_optional_env_var("VALIDATOR_PYLON_IDENTITY_TOKEN")
        cert_path = get_optional_env_var("VALIDATOR_MTLS_CERT_PATH")
        key_path = get_optional_env_var("VALIDATOR_MTLS_KEY_PATH")
        neurons_file = get_optional_env_var("VALIDATOR_NEURONS_FILE")

        if (identity_name is None) != (identity_token is None):
            raise SubnetMisconfiguredException(
                "Pylon identity configuration must provide both name and token. "
                "Expected VALIDATOR_PYLON_IDENTITY_NAME together with VALIDATOR_PYLON_IDENTITY_TOKEN."
            )

        return AsyncPylonClient(
            AsyncConfig(
                address=address,
                open_access_token=PylonAuthToken(open_access_token),
                identity_name=IdentityName(identity_name) if identity_name is not None else None,
                identity_token=PylonAuthToken(identity_token) if identity_token is not None else None,
                mtls_cert_path=cert_path,
                mtls_key_path=key_path,
                neurons_file=neurons_file,
            )
        )


DEFAULT_ASYNC_PYLON_CLIENT_PROVIDER: AsyncPylonClientProvider = EnvAsyncPylonClientProvider()
