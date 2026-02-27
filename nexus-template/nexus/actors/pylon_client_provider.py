from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Protocol, override

from pylon_client.artanis import Config, NetUid, PylonAuthToken, PylonClient
from pylon_client.artanis.v1 import GetNeuronsResponse

from nexus.utils.exceptions import InternalFrameworkException


class OpenAccessPylonApiLike(Protocol):
    def get_recent_neurons(self, netuid: NetUid) -> GetNeuronsResponse: ...


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


class StaticConfigPylonClientProvider(PylonClientProvider):
    address: str
    open_access_token: str

    def __init__(self, *, pylon_service_address: str, open_access_token: str) -> None:
        if not pylon_service_address:
            raise InternalFrameworkException("pylon service address cannot be empty")
        if not open_access_token:
            raise InternalFrameworkException("open_access_token cannot be empty")
        self.address = pylon_service_address
        self.open_access_token = open_access_token

    @override
    def get_client(self) -> SyncPylonClientLike:
        return PylonClient(
            Config(
                address=self.address,
                open_access_token=PylonAuthToken(self.open_access_token),
            )
        )
