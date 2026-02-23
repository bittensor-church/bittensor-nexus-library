# pyright: basic

from __future__ import annotations

from dataclasses import dataclass
from typing import override

from polyfactory.factories.pydantic_factory import ModelFactory
from pylon_client.artanis import BlockHash, BlockNumber, NetUid
from pylon_client.artanis.v1 import Block, GetNeuronsResponse, Neuron

from nexus.actors.pylon_client_provider import (
    OpenAccessPylonApiLike,
    PylonClientProvider,
    SyncPylonClientLike,
)


class NeuronFactory(ModelFactory[Neuron]):
    __model__ = Neuron


def build_neuron(*, uid: int, hotkey: str, validator_permit: bool) -> Neuron:
    return NeuronFactory.build(
        uid=uid,
        hotkey=hotkey,
        coldkey=f"cold-{hotkey}",
        validator_permit=validator_permit,
    )


@dataclass(frozen=True)
class FakeClientNamespace:
    open_access: FakeOpenAccessApi


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


class FakePylonClient(SyncPylonClientLike):
    def __init__(self, *, neurons: list[Neuron], netuid_calls: list[int]) -> None:
        self._open_access = FakeOpenAccessApi(neurons=neurons, netuid_calls=netuid_calls)
        self.v1 = FakeClientNamespace(open_access=self._open_access)

    @property
    def open_access(self) -> FakeOpenAccessApi:
        return self._open_access

    def __enter__(self) -> FakePylonClient:
        return self

    def __exit__(
        self,
        exc_type: object,
        exc_val: object,
        exc_tb: object,
    ) -> object:
        return None


class FakePylonClientProvider(PylonClientProvider):
    neurons: list[Neuron]
    netuid_calls: list[int]

    def __init__(self, *, neurons: list[Neuron]) -> None:
        self.neurons = neurons
        self.netuid_calls = []

    @override
    def get_client(self) -> SyncPylonClientLike:
        return FakePylonClient(neurons=self.neurons, netuid_calls=self.netuid_calls)
