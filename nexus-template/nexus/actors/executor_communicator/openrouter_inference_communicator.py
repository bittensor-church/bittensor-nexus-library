from __future__ import annotations

from typing import Any, override

from pydantic import BaseModel

from nexus.actors.neuron_router import Routed
from nexus.actors.openrouter_client_provider import DEFAULT_OPENROUTER_CLIENT_PROVIDER, OpenRouterClientProvider
from nexus.actors.openrouter_payload_creator import OpenRouterInferenceRequest
from nexus.core.runtime.actor import Actor, ActorBuilder
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.events import MessagesToSend, PipeToBus, ReceiveEvent
from nexus.utils.exceptions import NexusException

from .base_communicator import CommunicatorActor, ExecutorCommunicator


class OpenRouterInferenceExecutorFailure(NexusException):
    """Raised when OpenRouter inference fails for a specific executor input."""


class OpenRouterInferenceCommunicator[OutputModel: BaseModel](
    ExecutorCommunicator[OpenRouterInferenceRequest, OutputModel],
    ActorBuilder,
):
    """Run OpenRouter inference locally and validate the structured response."""

    openrouter_client_provider: OpenRouterClientProvider

    def __init__(
        self,
        _id: str,
        *,
        output_model: type[OutputModel],
        openrouter_client_provider: OpenRouterClientProvider | None = None,
    ) -> None:
        super().__init__(_id, input_model=OpenRouterInferenceRequest, output_model=output_model)
        self.openrouter_client_provider = openrouter_client_provider or DEFAULT_OPENROUTER_CLIENT_PROVIDER

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return OpenRouterInferenceCommunicatorActor[OutputModel](
            spec=self,
            pipe_to_bus=pipe_to_bus,
            context_store=context_store,
        )


class OpenRouterInferenceCommunicatorActor[OutputModel: BaseModel](
    CommunicatorActor[OpenRouterInferenceRequest, OutputModel]
):
    """Runtime actor for OpenRouter-backed inference communication."""

    communicator_spec: OpenRouterInferenceCommunicator[OutputModel]

    def __init__(
        self,
        *,
        spec: OpenRouterInferenceCommunicator[OutputModel],
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.communicator_spec = spec

    @override
    def handle_input(self, ctx: Context, event: ReceiveEvent[Routed[OpenRouterInferenceRequest]]) -> MessagesToSend:
        del ctx
        routed_input = event.payload
        try:
            openrouter_client = self.communicator_spec.openrouter_client_provider.get_client()
        except NexusException as exc:
            return self._internal_error_event(event.ctx_id, exc)

        try:
            messages: list[dict[str, Any]] = [dict(message) for message in routed_input.input.messages]
            output = openrouter_client.query(
                messages=messages,
                response_model=self.communicator_spec.output_model,
            )
            return self._processed_event(event.ctx_id, output)
        except Exception as exc:
            if isinstance(exc, NexusException):
                executor_error = exc
            else:
                executor_error = OpenRouterInferenceExecutorFailure("failure during OpenRouter inference")
                executor_error.__cause__ = exc
            return self._executor_error_event(event.ctx_id, executor_error)


__all__ = [
    "OpenRouterInferenceCommunicator",
    "OpenRouterInferenceCommunicatorActor",
    "OpenRouterInferenceExecutorFailure",
]
