import json
import logging
from threading import Lock
from typing import NewType, override

BlockNumber = NewType("BlockNumber", int)

from websockets.exceptions import ConnectionClosed, ConnectionClosedOK
from websockets.sync.client import ClientConnection, connect

from nexus.core.dsl.nodes import Source
from nexus.core.runtime.actor import ActorBuilder
from nexus.core.runtime.actor_patterns import ProducerActor
from nexus.core.runtime.context_store import ContextStore
from nexus.core.runtime.events import PipeToBus
from nexus.logging_utils import get_logger

logger: logging.Logger = get_logger(__name__)


class BlockScheduler(Source[BlockNumber], ActorBuilder):
    ws_url: str

    def __init__(self, _id: str, *, ws_url: str) -> None:
        Source[BlockNumber].__init__(self, _id)
        self.ws_url = ws_url

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> "BlockSchedulerActor":
        return BlockSchedulerActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class BlockSchedulerActor(ProducerActor[BlockNumber]):
    def __init__(self, spec: BlockScheduler, pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self._ws_url = spec.ws_url
        self._ws: ClientConnection | None = None
        self._ws_lock = Lock()

    @override
    def _produce(self) -> None:
        ws = connect(self._ws_url)
        with self._ws_lock:
            self._ws = ws

        ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "chain_subscribeNewHeads", "params": []}))
        ws.recv()  # subscription ack

        try:
            for raw in ws:
                msg = json.loads(raw)
                block_hex: str = msg["params"]["result"]["number"]
                block_number = BlockNumber(int(block_hex, 16))
                logger.info("Block #%d", block_number)
                self._emit(block_number)
        except ConnectionClosedOK:
            logger.info("WebSocket closed normally")
        except ConnectionClosed as exc:
            logger.warning("WebSocket closed abnormally: %s", exc)

    @override
    def _on_stop(self) -> None:
        with self._ws_lock:
            ws = self._ws
        if ws is not None:
            ws.close()
