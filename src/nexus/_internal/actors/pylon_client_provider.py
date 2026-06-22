from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager
from typing import Protocol, override

import httpx
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
from pylon_client.artanis.v1 import GetLatestBlockInfoResponse, GetNeuronsResponse, Neuron, SetWeightsResponse

from nexus._internal.utils.pylon_client_settings import PylonClientSettingsMixin
from nexus._internal.utils.subnet_settings import get_subnet_settings_as


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


class AsyncPylonClientLike(Protocol):
    """
    Protocol for async Pylon client. Only includes the parts of the client used by the sender runtime.

    this is a workaround to avoid importing the actual AsyncPylonClient in actors,
    which would make testing harder and create unnecessary dependencies.
    This should be removed once pylon client provides a full-fledged mock
    to be used in contexts like this.
    """

    async def __aenter__(self) -> AsyncPylonClientLike: ...

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None: ...

    def get_neuron_client(
        self, neuron: Neuron, timeout: float = 30.0
    ) -> AbstractAsyncContextManager[httpx.AsyncClient]: ...


class PylonClientProvider(ABC):
    """Provides sync Pylon clients to actors that read or write chain state."""

    @abstractmethod
    def get_client(self) -> SyncPylonClientLike: ...


class EnvPylonClientProvider(PylonClientProvider):
    """Builds Pylon clients from validator environment variables."""

    @override
    def get_client(self) -> SyncPylonClientLike:
        settings = get_subnet_settings_as(PylonClientSettingsMixin)

        return PylonClient(
            Config(
                address=settings.pylon_service_address,
                open_access_token=PylonAuthToken(settings.pylon_open_access_token),
                identity_name=IdentityName(settings.pylon_identity_name)
                if settings.pylon_identity_name is not None
                else None,
                identity_token=PylonAuthToken(settings.pylon_identity_token)
                if settings.pylon_identity_token is not None
                else None,
                mtls_cert_path=settings.mtls_cert_path,
                mtls_key_path=settings.mtls_key_path,
                neurons_file=settings.neurons_file,
                neuron_keepalive_expiry=settings.neuron_keepalive_expiry,
            )
        )


DEFAULT_PYLON_CLIENT_PROVIDER: PylonClientProvider = EnvPylonClientProvider()


class AsyncPylonClientProvider(ABC):
    """Provides async Pylon clients to actors that query miners or read chain state."""

    @abstractmethod
    def get_client(self) -> AsyncPylonClientLike: ...


class EnvAsyncPylonClientProvider(AsyncPylonClientProvider):
    """
    Builds async Pylon clients from validator environment variables.

    Set ``VALIDATOR_MTLS_CERT_PATH`` and ``VALIDATOR_MTLS_KEY_PATH`` to enable
    mTLS when querying neurons via the `get_neuron_client` method. When absent, falls back to plain HTTP.
    """

    @override
    def get_client(self) -> AsyncPylonClientLike:
        settings = get_subnet_settings_as(PylonClientSettingsMixin)

        return AsyncPylonClient(
            AsyncConfig(
                address=settings.pylon_service_address,
                open_access_token=PylonAuthToken(settings.pylon_open_access_token),
                identity_name=IdentityName(settings.pylon_identity_name)
                if settings.pylon_identity_name is not None
                else None,
                identity_token=PylonAuthToken(settings.pylon_identity_token)
                if settings.pylon_identity_token is not None
                else None,
                mtls_cert_path=settings.mtls_cert_path,
                mtls_key_path=settings.mtls_key_path,
                neurons_file=settings.neurons_file,
                neuron_keepalive_expiry=settings.neuron_keepalive_expiry,
            )
        )


DEFAULT_ASYNC_PYLON_CLIENT_PROVIDER: AsyncPylonClientProvider = EnvAsyncPylonClientProvider()
