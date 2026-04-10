"""Typed OpenRouter selection models and rendering helpers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue

type ScalarValue = str | int | float | bool | None
type OpenRouterMessageContent = dict[str, JsonValue]


def text_content_block(text: str) -> OpenRouterMessageContent:
    return {"type": "text", "text": text}


def _label_block(*, index: int, field_name: str) -> OpenRouterMessageContent:
    return text_content_block(f"item[{index}].{field_name}:")


class SelectionBase(BaseModel, ABC):
    """Base class for persisted OpenRouter selection values."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    @abstractmethod
    def render_openrouter_content(
        self,
        *,
        index: int,
        field_name: str,
    ) -> list[OpenRouterMessageContent]:
        """Render the stored selection into OpenRouter message content blocks."""


class ScalarSelection(SelectionBase):
    """Scalar selection rendered as plain text."""

    kind: Literal["scalar"] = "scalar"
    value: ScalarValue

    def render_openrouter_content(
        self,
        *,
        index: int,
        field_name: str,
    ) -> list[OpenRouterMessageContent]:
        return [text_content_block(f"item[{index}].{field_name}: {self.rendered_value()}")]

    def rendered_value(self) -> str:
        return str(self.value)


class ImageUrlSelection(SelectionBase):
    """Typed selector value for an OpenRouter ``image_url`` content block."""

    kind: Literal["image_url"] = "image_url"
    url: str

    def render_openrouter_content(
        self,
        *,
        index: int,
        field_name: str,
    ) -> list[OpenRouterMessageContent]:
        return [_label_block(index=index, field_name=field_name), {"type": "image_url", "image_url": {"url": self.url}}]


class FileSelection(SelectionBase):
    """Typed selector value for an OpenRouter ``file`` content block."""

    kind: Literal["file"] = "file"
    filename: str
    file_data: str

    def render_openrouter_content(
        self,
        *,
        index: int,
        field_name: str,
    ) -> list[OpenRouterMessageContent]:
        return [
            _label_block(index=index, field_name=field_name),
            {
                "type": "file",
                "file": {
                    "filename": self.filename,
                    "file_data": self.file_data,
                },
            },
        ]


class InputAudioSelection(SelectionBase):
    """Typed selector value for an OpenRouter ``input_audio`` content block."""

    kind: Literal["input_audio"] = "input_audio"
    data: str
    format: str

    def render_openrouter_content(
        self,
        *,
        index: int,
        field_name: str,
    ) -> list[OpenRouterMessageContent]:
        return [
            _label_block(index=index, field_name=field_name),
            {
                "type": "input_audio",
                "input_audio": {
                    "data": self.data,
                    "format": self.format,
                },
            },
        ]


class VideoUrlSelection(SelectionBase):
    """Typed selector value for an OpenRouter ``video_url`` content block."""

    kind: Literal["video_url"] = "video_url"
    url: str

    def render_openrouter_content(
        self,
        *,
        index: int,
        field_name: str,
    ) -> list[OpenRouterMessageContent]:
        return [_label_block(index=index, field_name=field_name), {"type": "video_url", "video_url": {"url": self.url}}]


type MultimodalSelection = ImageUrlSelection | FileSelection | InputAudioSelection | VideoUrlSelection
type SelectionValue = Annotated[
    ScalarSelection | ImageUrlSelection | FileSelection | InputAudioSelection | VideoUrlSelection,
    Field(discriminator="kind"),
]


class SelectedItem(BaseModel):
    """Normalized per-item selection payload stored inside an OpenRouter request."""

    selected_fields: dict[str, SelectionValue]


__all__ = [
    "FileSelection",
    "ImageUrlSelection",
    "InputAudioSelection",
    "MultimodalSelection",
    "OpenRouterMessageContent",
    "ScalarSelection",
    "ScalarValue",
    "SelectedItem",
    "SelectionValue",
    "VideoUrlSelection",
    "text_content_block",
]
