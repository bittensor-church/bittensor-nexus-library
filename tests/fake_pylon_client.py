# pyright: basic

from __future__ import annotations

from dataclasses import dataclass
from typing import override

from pylon_client.artanis import BlockHash, BlockNumber, MechanismId, NetUid, Timestamp
from pylon_client.artanis.unstable import GetWeightsStatusResponse
from pylon_client.artanis.v1 import (
    Block,
    GetLatestBlockInfoResponse,
    GetNeuronsResponse,
    Neuron,
    SetWeightsResponse,
)

from nexus.v1 import (
    Hotkey,
    IdentityPylonApiLike,
    OpenAccessPylonApiLike,
    PylonClientProvider,
    SyncPylonClientLike,
    UnstableIdentityPylonApiLike,
    Weight,
)


@dataclass(frozen=True)
class FakeV1Namespace:
    open_access: FakeOpenAccessApi


@dataclass(frozen=True)
class FakeUnstablePylonNamespace:
    identity: FakeUnstableIdentityApi


class FakeOpenAccessApi(OpenAccessPylonApiLike):
    neurons: list[Neuron]
    netuid_calls: list[int]

    def __init__(self, *, neurons: list[Neuron], netuid_calls: list[int]) -> None:
        self.neurons = neurons
        self.netuid_calls = netuid_calls

    def get_recent_neurons(self, netuid: NetUid) -> GetNeuronsResponse:
        self.netuid_calls.append(int(netuid))
        return GetNeuronsResponse(
            block=Block(number=BlockNumber(0), hash=BlockHash("0x0")),
            neurons={neuron.hotkey: neuron for neuron in self.neurons},
        )

    def get_latest_block_info(self) -> GetLatestBlockInfoResponse:
        return GetLatestBlockInfoResponse(
            number=BlockNumber(0),
            timestamp=Timestamp(0),
            hash=BlockHash("0x0"),
        )


class FakeIdentityApi(IdentityPylonApiLike):
    def put_weights(self, weights: dict[Hotkey, Weight]) -> SetWeightsResponse:
        return SetWeightsResponse()


class FakeUnstableIdentityApi(UnstableIdentityPylonApiLike):
    def get_weights_status(
        self,
        block_number: BlockNumber,
        mechanism_id: MechanismId = MechanismId(0),
    ) -> GetWeightsStatusResponse:
        return GetWeightsStatusResponse(weights_submitted=False)


class FakePylonClient(SyncPylonClientLike):
    def __init__(self, *, neurons: list[Neuron], netuid_calls: list[int]) -> None:
        self._open_access = FakeOpenAccessApi(neurons=neurons, netuid_calls=netuid_calls)
        self._identity = FakeIdentityApi()
        self._unstable_identity = FakeUnstableIdentityApi()
        self._v1 = FakeV1Namespace(open_access=self._open_access)
        self._unstable = FakeUnstablePylonNamespace(identity=self._unstable_identity)

    @property
    def v1(self) -> FakeV1Namespace:
        return self._v1

    @property
    def open_access(self) -> FakeOpenAccessApi:
        return self._open_access

    @property
    def identity(self) -> FakeIdentityApi:
        return self._identity

    @property
    def unstable(self) -> FakeUnstablePylonNamespace:
        return self._unstable

    def __enter__(self) -> FakePylonClient:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> object:
        pass


class FakePylonClientProvider(PylonClientProvider):
    neurons: list[Neuron]
    netuid_calls: list[int]

    def __init__(self, *, neurons: list[Neuron]) -> None:
        self.neurons = neurons
        self.netuid_calls = []

    @override
    def get_client(self) -> SyncPylonClientLike:
        return FakePylonClient(neurons=self.neurons, netuid_calls=self.netuid_calls)
