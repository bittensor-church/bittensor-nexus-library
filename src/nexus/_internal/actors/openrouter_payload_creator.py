from collections.abc import Callable, Mapping
from typing import Literal, TypedDict, cast, override

from pydantic import BaseModel, ValidationError, field_validator

from nexus._internal.actors.openrouter_selection import (
    Fields,
    FieldValue,
    FileField,
    ImageUrlField,
    InputAudioField,
    OpenRouterMessageContent,
    ScalarField,
    VideoUrlField,
    text_content_block,
)
from nexus._internal.actors.payload_creator import PayloadCreator
from nexus._internal.core.runtime.actor import Actor, ActorBuilder
from nexus._internal.core.runtime.actor_patterns import TransformActor
from nexus._internal.core.runtime.context_store import Context, ContextStore
from nexus._internal.core.runtime.events import PipeToBus


class OpenRouterUserMessage(TypedDict):
    """OpenRouter user-role message assembled from normalized selected items."""

    role: Literal["user"]
    content: list[OpenRouterMessageContent]


def _is_string_mapping(value: object, *, keys: set[str]) -> bool:
    if not isinstance(value, Mapping):
        return False

    typed_value = cast(Mapping[object, object], value)
    return set(typed_value.keys()) == keys and all(isinstance(item, str) for item in typed_value.values())


def _validate_openrouter_message_content(block: OpenRouterMessageContent) -> OpenRouterMessageContent:
    block_type = block.get("type")
    if block_type == "text":
        if set(block) != {"type", "text"} or not isinstance(block.get("text"), str):
            raise ValueError(f"Malformed OpenRouter content block: {block!r}")
        return block

    if block_type == "image_url":
        if set(block) != {"type", "image_url"} or not _is_string_mapping(block.get("image_url"), keys={"url"}):
            raise ValueError(f"Malformed OpenRouter content block: {block!r}")
        return block

    if block_type == "file":
        if set(block) != {"type", "file"} or not _is_string_mapping(
            block.get("file"),
            keys={"filename", "file_data"},
        ):
            raise ValueError(f"Malformed OpenRouter content block: {block!r}")
        return block

    if block_type == "input_audio":
        if set(block) != {"type", "input_audio"} or not _is_string_mapping(
            block.get("input_audio"),
            keys={"data", "format"},
        ):
            raise ValueError(f"Malformed OpenRouter content block: {block!r}")
        return block

    if block_type == "video_url":
        if set(block) != {"type", "video_url"} or not _is_string_mapping(block.get("video_url"), keys={"url"}):
            raise ValueError(f"Malformed OpenRouter content block: {block!r}")
        return block

    raise ValueError(f"Malformed OpenRouter content block: {block!r}")


class OpenRouterInferenceRequest(BaseModel):
    """Persisted OpenRouter executor payload containing selections and rendered messages."""

    fields: tuple[Fields, ...]
    messages: tuple[OpenRouterUserMessage, ...]

    @field_validator("messages")
    @classmethod
    def _validate_messages(cls, messages: tuple[OpenRouterUserMessage, ...]) -> tuple[OpenRouterUserMessage, ...]:
        for message in messages:
            for block in message["content"]:
                _validate_openrouter_message_content(block)
        return messages


class MultiOpenRouterPayloadCreator[Item](
    PayloadCreator[tuple[Item, ...], OpenRouterInferenceRequest],
    ActorBuilder,
):
    """
    Build an OpenRouter request from a tuple input using typed multimodal selections.

    ``item_selector`` may return ``None`` to skip one input item. The creator raises
    ``ValueError`` if every input item is skipped and no selected items remain.
    """

    item_selector: Callable[[Item], Mapping[str, FieldValue] | None]
    user_prompt: str

    def __init__(
        self,
        _id: str,
        *,
        item_selector: Callable[[Item], Mapping[str, FieldValue] | None],
        user_prompt: str = "Selected items:",
    ) -> None:
        super().__init__(_id)
        self.item_selector = item_selector
        self.user_prompt = user_prompt

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return MultiOpenRouterPayloadCreatorActor[Item](
            spec=self,
            pipe_to_bus=pipe_to_bus,
            context_store=context_store,
        )


class MultiOpenRouterPayloadCreatorActor[Item](TransformActor[tuple[Item, ...], OpenRouterInferenceRequest]):
    """Runtime actor that normalizes selected fields and renders OpenRouter messages."""

    creator_spec: MultiOpenRouterPayloadCreator[Item]

    def __init__(
        self,
        *,
        spec: MultiOpenRouterPayloadCreator[Item],
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.creator_spec = spec

    @override
    def _transform(self, ctx: Context, payload: tuple[Item, ...]) -> OpenRouterInferenceRequest:
        del ctx
        selected_items = tuple(
            selected_item for item in payload if (selected_item := self._build_selected_item(item)) is not None
        )
        if len(selected_items) == 0:
            raise ValueError("MultiOpenRouterPayloadCreator requires at least one selected item")
        return OpenRouterInferenceRequest(
            fields=selected_items,
            messages=(
                {
                    "role": "user",
                    "content": self._render_content(selected_items),
                },
            ),
        )

    def _build_selected_item(self, item: Item) -> Fields | None:
        selected_fields = self.creator_spec.item_selector(item)
        if selected_fields is None:
            return None
        try:
            return Fields(fields=dict(selected_fields))
        except ValidationError as exc:
            field_name = self._invalid_field_name(exc)
            raise ValueError(f"Malformed selection for field '{field_name}': {exc}") from exc

    def _render_content(self, selected_items: tuple[Fields, ...]) -> list[OpenRouterMessageContent]:
        content: list[OpenRouterMessageContent] = [text_content_block(self.creator_spec.user_prompt)]
        for index, selected_item in enumerate(selected_items):
            for field_name, value in selected_item.fields.items():
                content.extend(value.render_openrouter_content(index=index, field_name=field_name))
        return content

    def _invalid_field_name(self, exc: ValidationError) -> str:
        for error in exc.errors():
            loc = error.get("loc", ())
            if len(loc) >= 2 and loc[0] == "fields":
                return str(loc[1])
        return "<unknown>"


__all__ = [
    "FileField",
    "ImageUrlField",
    "InputAudioField",
    "MultiOpenRouterPayloadCreator",
    "OpenRouterInferenceRequest",
    "ScalarField",
    "FieldValue",
    "Fields",
    "VideoUrlField",
]
