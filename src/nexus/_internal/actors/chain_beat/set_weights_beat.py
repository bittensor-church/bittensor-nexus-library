from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, override

from pylon_client.artanis import BasePylonException

from nexus._internal.actors.pylon_client_provider import DEFAULT_PYLON_CLIENT_PROVIDER, PylonClientProvider
from nexus._internal.core.dsl.nodes import (
    Node,
    NodeSinks,
    NodeSources,
    Sink,
    SinkName,
    Source,
    SourceName,
)
from nexus._internal.core.runtime.actor import Actor, ActorBuilder, EventHandler
from nexus._internal.core.runtime.context_store import Context, ContextStore
from nexus._internal.core.runtime.events import MessagesToSend, PipeToBus, ReceiveEvent, SendEvent
from nexus._internal.logging_utils import get_logger
from nexus._internal.utils.chain import DEFAULT_TEMPO, get_epoch_containing_block
from nexus._internal.utils.types import BlockCount, BlockNumber, Epoch, NetUid, Tempo

from .block_beat import BlockBeat

if TYPE_CHECKING:
    from nexus._internal.actors.weight_setter import WeightSettingSuccess

logger: logging.Logger = get_logger(__name__)


@dataclass(frozen=True)
class SetWeightsBeat:
    """Event signalling that weights should be set on-chain for the given epoch."""

    epoch: Epoch
    block_number: BlockNumber


class SetWeightsBeatNode(Node, ActorBuilder):
    """
    Gates attempts to set weights within an epoch.

    Consumes BlockBeat (from BlockBeatNode) and WeightSettingSuccess (from WeightSetterNode).
    Emits SetWeightsBeat only if all conditions are met:
      1. Current block is at least `epoch_start_offset` blocks past the epoch start.
      2. No WeightSettingSuccess has been received yet in the current epoch (in-memory flag).
      3. The last emitted SetWeightsBeat was at least `attempts_cooldown` blocks ago.
      4. pylon.identity.get_weights_status returns weights_set=False for the current block.

    sink block_beat: BlockBeat triggering condition evaluation
    sink weights_set: WeightSettingSuccess marking the epoch as satisfied
    source source: SetWeightsBeat when all conditions are met
    """

    block_beat: Sink[BlockBeat]
    weights_set: Sink[WeightSettingSuccess]
    source: Source[SetWeightsBeat]

    netuid: NetUid
    epoch_start_offset: BlockCount
    attempts_cooldown: BlockCount
    tempo: Tempo
    pylon_client_provider: PylonClientProvider

    def __init__(
        self,
        _id: str,
        *,
        netuid: NetUid,
        epoch_start_offset: BlockCount,
        attempts_cooldown: BlockCount = BlockCount(4),  # noqa: B008
        tempo: Tempo = DEFAULT_TEMPO,
        pylon_client_provider: PylonClientProvider | None = None,
    ) -> None:
        """
        Args:
            _id: Node ID / name.
            netuid: Subnet number for epoch derivation and weights status queries.
            epoch_start_offset: Minimum blocks since epoch start before the first beat.
            attempts_cooldown: Minimum blocks between consecutive emitted beats.
            tempo: Subnet tempo used when deriving the epoch from a block number.
            pylon_client_provider: Provider for pylon client instances.

        """
        super().__init__(_id)
        self.netuid = netuid
        self.epoch_start_offset = epoch_start_offset
        self.attempts_cooldown = attempts_cooldown
        self.tempo = tempo
        self.pylon_client_provider = pylon_client_provider or DEFAULT_PYLON_CLIENT_PROVIDER

        self.block_beat = Sink[BlockBeat](f"{_id}-block-beat-sink", owner_node=self)
        self.weights_set = Sink(f"{_id}-weights-set-sink", owner_node=self)
        self.source = Source[SetWeightsBeat](f"{_id}-source", owner_node=self)

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks(
            sinks={
                SinkName("block_beat"): self.block_beat,
                SinkName("weights_set"): self.weights_set,
            }
        )

    @override
    def sources(self) -> NodeSources:
        return NodeSources(sources={SourceName("source"): self.source})

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> SetWeightsBeatActor:
        return SetWeightsBeatActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class SetWeightsBeatActor(Actor):
    """Runtime counterpart of SetWeightsBeatNode."""

    spec: SetWeightsBeatNode
    _last_success_epoch: Epoch | None
    _last_beat_at_block: BlockNumber | None

    def __init__(self, spec: SetWeightsBeatNode, pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(name=spec.id, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.spec = spec
        self._last_success_epoch = None
        self._last_beat_at_block = None

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {
            self.spec.block_beat: self._on_block_beat,
            self.spec.weights_set: self._on_weights_set,
        }

    def _on_weights_set(self, _ctx: Context, event: ReceiveEvent[Any]) -> MessagesToSend:
        success: WeightSettingSuccess = event.payload
        self._last_success_epoch = success.epoch
        return ()

    def _on_block_beat(self, _ctx: Context, event: ReceiveEvent[BlockBeat]) -> MessagesToSend:
        block_number = event.payload.block_number
        epoch = get_epoch_containing_block(block=block_number, netuid=self.spec.netuid, tempo=self.spec.tempo)

        if block_number - epoch.first_block < self.spec.epoch_start_offset:
            return ()

        if self._last_success_epoch == epoch:
            return ()

        if (
            self._last_beat_at_block is not None
            and block_number - self._last_beat_at_block < self.spec.attempts_cooldown
        ):
            return ()

        # 1. Retry on BasePylonException - request/timeout/response failures may be transient.
        # 2. Bubble up all non-Pylon exceptions - actor loop will log them and keep running.
        pylon = self.spec.pylon_client_provider.get_client()
        try:
            with pylon:
                status = pylon.identity.get_weights_status(block_number=block_number)
        except BasePylonException as exc:
            logger.warning(
                "Pylon related failure; will retry on the next block. error_type=%s error=%s",
                type(exc).__name__,
                exc,
            )
            return ()

        if status.weights_set:
            self._last_success_epoch = epoch
            return ()

        self._last_beat_at_block = block_number
        logger.info(f"Emitting SetWeightsBeat for epoch {epoch} at block {block_number}")
        return (
            SendEvent(
                ctx_id=event.ctx_id,
                source=self.spec.source,
                payload=SetWeightsBeat(epoch=epoch, block_number=block_number),
            ),
        )
