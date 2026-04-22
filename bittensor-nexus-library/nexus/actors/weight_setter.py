from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, override

from pylon_client.artanis import PylonMisconfigured, PylonResponseException

from nexus.core.dsl.nodes import Transform
from nexus.core.runtime.actor import ActorBuilder
from nexus.core.runtime.actor_patterns import TransformActor
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.events import PipeToBus
from nexus.logging_utils import get_logger

from ..core.runtime.task_result_store import TaskResultStore
from ..utils.exceptions import WeightSettingException
from ..utils.types import Epoch, Hotkey, Weight
from .chain_beat.epoch_beat import EpochBeat
from .pylon_client_provider import DEFAULT_PYLON_CLIENT_PROVIDER, PylonClientProvider
from .task_result_store_provider import DEFAULT_TASK_RESULT_STORE_PROVIDER, TaskResultStoreProvider

logger: logging.Logger = get_logger(__name__)


type WeighingFunc = Callable[[WeightsCalculationBundle], Mapping[Hotkey, Weight]]


@dataclass(frozen=True)
class WeightSettingSuccess:
    pass


@dataclass(frozen=True)
class WeightsCalculationBundle:
    """
    Bundle of data and data sources useful for weight calculation.
    """

    epoch: Epoch
    tasks_result_store: TaskResultStore[Any, Any, Any]


class WeightSetterNode(Transform[EpochBeat, WeightSettingSuccess], ActorBuilder):
    """Calculates neuron weights using the provided weighing function and sets them on-chain via pylon.
    Triggered by epoch beats. Weights are arbitrary non-negative floats, set for all neurons in one go.
    No normalization needed. Note that subnet hyperparams may further influence the effective weights.

    sink sink: EpochBeat triggering weight calculation
    source ok: WeightSettingSuccess on successful commit
    source error: WeightSettingException on failure
    """

    weighing_func: WeighingFunc
    pylon_client_provider: PylonClientProvider
    task_result_store_provider: TaskResultStoreProvider[Any, Any, Any]

    def __init__(
        self,
        _id: str,
        *,
        weighing_func: WeighingFunc,
        pylon_client_provider: PylonClientProvider | None = None,
        task_result_store_provider: TaskResultStoreProvider[Any, Any, Any] | None = None,
    ) -> None:
        super().__init__(_id)
        self.weighing_func = weighing_func
        self.pylon_client_provider = pylon_client_provider or DEFAULT_PYLON_CLIENT_PROVIDER
        self.task_result_store_provider = task_result_store_provider or DEFAULT_TASK_RESULT_STORE_PROVIDER

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> WeightSetterActor:
        return WeightSetterActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class WeightSetterActor(TransformActor[EpochBeat, WeightSettingSuccess]):
    weight_setter_spec: WeightSetterNode

    def __init__(self, spec: WeightSetterNode, pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.weight_setter_spec = spec

    @override
    def _transform(self, ctx: Context, payload: EpochBeat) -> WeightSettingSuccess:
        pylon = self.weight_setter_spec.pylon_client_provider.get_client()
        bundle = self._prepare_calculation_bundle(beat=payload)

        logger.info(f"Setting weights for epoch {payload.epoch}")

        try:
            weights = self.weight_setter_spec.weighing_func(bundle)
        except Exception as exc:
            raise WeightSettingException(f"Failed to calculate weights for epoch {payload.epoch}") from exc

        try:
            with pylon:
                pylon.identity.put_weights({**weights})
        except (PylonResponseException, PylonMisconfigured) as exc:
            raise WeightSettingException(f"Failed to set weights for epoch {payload.epoch}") from exc

        return WeightSettingSuccess()

    def _prepare_calculation_bundle(self, beat: EpochBeat) -> WeightsCalculationBundle:
        return WeightsCalculationBundle(
            epoch=beat.epoch,
            tasks_result_store=self.weight_setter_spec.task_result_store_provider.get_task_result_store(),
        )
