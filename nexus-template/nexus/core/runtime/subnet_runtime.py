from __future__ import annotations

from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import Thread
from time import monotonic

from nexus.actors import BlockBeatNode, PylonClientProvider
from nexus.core.dsl.flow import Flow
from nexus.core.dsl.nodes import Node
from nexus.core.dsl.piping import Piping

from .actor import Actor, ActorBuilder
from .context_store import ContextStore, InMemoryContextStorePersistence
from .event_bus import EventBus
from .events import PipeToBus

type ActorFactory = Callable[[PipeToBus, ContextStore], Actor]


@dataclass
class SubnetRuntime:
    """
    Runtime components required to run a subnet graph.

    A SubnetRuntime is one-off: `run_loop()` can be called only once for a given
    instance, and actor/event-bus threads are not restartable after shutdown.
    """

    actors: tuple[Actor, ...]
    piping: Piping
    pipe_to_bus: PipeToBus
    context_store: ContextStore
    event_bus: EventBus
    _threads: tuple[Thread, ...] = field(default=(), init=False, repr=False)
    _is_started: bool = field(default=False, init=False, repr=False)
    _stop_requested: bool = field(default=False, init=False, repr=False)

    def run_loop(self) -> None:
        if self._is_started:
            raise RuntimeError("SubnetRuntime run_loop() has already been called for this instance.")

        actor_threads = [actor.run_loop() for actor in dict.fromkeys(self.actors)]
        event_bus_thread = self.event_bus.run_loop()
        self._threads = tuple(actor_threads + [event_bus_thread])
        self._is_started = True

    def request_stop(self) -> None:
        if not self._is_started:
            raise RuntimeError("Cannot call request_stop() before run_loop().")
        if self._stop_requested:
            return
        self._stop_requested = True
        self.event_bus.request_stop()

    def wait_for_stop(self, timeout_seconds: float = 30.0) -> None:
        if timeout_seconds < 0:
            raise ValueError(f"Expected non-negative timeout_seconds, got {timeout_seconds}.")

        if len(self._threads) == 0:
            return

        deadline = monotonic() + timeout_seconds
        alive: list[str] = []
        for thread in self._threads:
            remaining = max(0.0, deadline - monotonic())
            thread.join(remaining)
            if thread.is_alive():
                alive.append(thread.name)

        if alive:
            raise TimeoutError(f"Timed out while waiting for runtime threads to stop: {alive}")

    @contextmanager
    def running(self, shutdown_timeout_seconds: float = 30.0) -> Generator[SubnetRuntime]:
        if shutdown_timeout_seconds < 0:
            raise ValueError(f"Expected non-negative shutdown_timeout_seconds, got {shutdown_timeout_seconds}.")

        self.run_loop()
        try:
            yield self
        except BaseException as body_exc:
            try:
                self.request_stop()
                self.wait_for_stop(timeout_seconds=shutdown_timeout_seconds)
            except BaseException as shutdown_exc:
                raise BaseExceptionGroup(
                    "SubnetRuntime body failed and shutdown failed",
                    [body_exc, shutdown_exc],
                ) from None
            raise
        else:
            self.request_stop()
            self.wait_for_stop(timeout_seconds=shutdown_timeout_seconds)


class SubnetBuilder:
    _nodes: tuple[Node, ...]
    _flows: list[Flow]
    _extra_actors: list[Actor]
    _extra_actor_factories: list[ActorFactory]
    _context_store: ContextStore
    _pipe_to_bus: PipeToBus
    _include_node_flows: bool
    _is_built: bool

    def __init__(
        self,
        *,
        nodes: Sequence[Node],
        context_store: ContextStore | None = None,
        pipe_to_bus: PipeToBus | None = None,
        include_node_flows: bool = True,
    ) -> None:
        self._nodes = tuple(nodes)
        self._flows = []
        self._extra_actors = []
        self._extra_actor_factories = []
        self._context_store = (
            context_store or ContextStore.recover_from(InMemoryContextStorePersistence()).context_store
        )
        self._pipe_to_bus = pipe_to_bus or PipeToBus()
        self._include_node_flows = include_node_flows
        self._is_built = False

    @property
    def context_store(self) -> ContextStore:
        return self._context_store

    @property
    def pipe_to_bus(self) -> PipeToBus:
        return self._pipe_to_bus

    def add_flows(self, *flows: Flow) -> SubnetBuilder:
        """Append one or more flows; call any number of times before `build()`."""
        self._ensure_not_built()
        self._flows.extend(flows)
        return self

    def add_actors(self, *actors: Actor) -> SubnetBuilder:
        """Append pre-built auxiliary actors to run alongside actors built from nodes."""
        self._ensure_not_built()
        self._extra_actors.extend(actors)
        return self

    def add_actor_factories(self, *actor_factories: ActorFactory) -> SubnetBuilder:
        """Append factories that create auxiliary actors during `build()`."""
        self._ensure_not_built()
        self._extra_actor_factories.extend(actor_factories)
        return self

    def build(self) -> SubnetRuntime:
        self._ensure_not_built()
        if len(self._flows) == 0:
            raise ValueError("Expected at least one flow; got an empty `flows` list.")

        piping = Piping()
        if self._include_node_flows:
            for node in self._nodes:
                piping.add_flow(Flow.from_connectable(node))
        for flow in self._flows:
            piping.add_flow(flow)

        built_actors: list[Actor] = []
        for node in self._nodes:
            if not isinstance(node, ActorBuilder):
                raise TypeError(f"Node {node.id!r} does not implement ActorBuilder; cannot build runtime actors.")
            built_actors.append(
                node.build_actor(
                    pipe_to_bus=self._pipe_to_bus,
                    context_store=self._context_store,
                )
            )

        extra_actors_from_factories = [
            factory(self._pipe_to_bus, self._context_store) for factory in self._extra_actor_factories
        ]
        all_actors = tuple([*built_actors, *self._extra_actors, *extra_actors_from_factories])
        event_bus = EventBus(
            connections=piping.pipes,
            input_pipe=self._pipe_to_bus,
            actors=list(all_actors),
            context_store=self._context_store,
        )

        self._is_built = True
        return SubnetRuntime(
            actors=all_actors,
            piping=piping,
            pipe_to_bus=self._pipe_to_bus,
            context_store=self._context_store,
            event_bus=event_bus,
        )

    def _ensure_not_built(self) -> None:
        if self._is_built:
            raise RuntimeError("SubnetBuilder is immutable after build(); create a new builder for another runtime.")
