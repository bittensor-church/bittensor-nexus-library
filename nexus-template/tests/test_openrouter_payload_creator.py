# pyright: basic

from dataclasses import dataclass
from typing import cast
from uuid import UUID

import pytest
from pydantic import BaseModel, ValidationError
from transform_test_utils import TransformActorTestSetupFactory
from utils import build_nexus_task_result, wait_until

from nexus.actors.openrouter_payload_creator import MultiOpenRouterPayloadCreator, OpenRouterInferenceRequest
from nexus.actors.openrouter_selection import (
    FileSelection,
    ImageUrlSelection,
    InputAudioSelection,
    ScalarSelection,
    VideoUrlSelection,
)
from nexus.actors.openrouter_task_result_selection import select_single_task_result_fields
from nexus.core.runtime.nexus_task_types import TaskResultId
from nexus.core.runtime.task_result_store import SingleTaskResult
from nexus.utils.exceptions import SafeInvokeWrappedException


class PromptItem(BaseModel):
    item_id: str
    title: str
    image_url: str


@dataclass(frozen=True)
class _UnsupportedSelectionObject:
    label: str = "unsupported"


def _sample_prompt_item() -> PromptItem:
    return PromptItem(item_id="a", title="Alpha", image_url="https://example.com/a.png")


def _image_selection(url: str) -> ImageUrlSelection:
    return ImageUrlSelection(url=url)


def _file_selection(*, filename: str, file_data: str) -> FileSelection:
    return FileSelection(filename=filename, file_data=file_data)


def _input_audio_selection(*, data: str, format: str) -> InputAudioSelection:
    return InputAudioSelection(data=data, format=format)


def _video_url_selection(url: str) -> VideoUrlSelection:
    return VideoUrlSelection(url=url)


def _scalar_selection(value: str | int | float | bool | None) -> ScalarSelection:
    return ScalarSelection(value=value)


def _run_payload_creator(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
    *,
    item_selector,
):
    creator = MultiOpenRouterPayloadCreator[PromptItem](
        "openrouter-payload-creator",
        item_selector=item_selector,
    )
    setup = transform_actor_test_setup_factory(creator)

    with setup.running():
        ctx_id = setup.send(input_payload=(_sample_prompt_item(),))
        wait_until(
            lambda: len(setup.processed_collector.received_events) + len(setup.error_collector.received_events) == 1
        )

    return ctx_id, setup


def test_multi_openrouter_payload_creator_renders_stable_multimodal_content(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    creator = MultiOpenRouterPayloadCreator[PromptItem](
        "openrouter-payload-creator",
        item_selector=lambda item: {
            "task_result_id": _scalar_selection(item.item_id),
            "title": _scalar_selection(item.title),
            "image": _image_selection(item.image_url),
        },
    )
    setup = transform_actor_test_setup_factory(creator)

    with setup.running():
        ctx_id = setup.send(
            input_payload=(PromptItem(item_id="a", title="Alpha", image_url="https://example.com/a.png"),)
        )
        wait_until(lambda: len(setup.processed_collector.received_events) == 1)

    assert len(setup.error_collector.received_events) == 0
    event = setup.processed_collector.received_events[0]
    request = event.payload

    assert event.ctx_id == ctx_id
    assert request.selected_items[0].selected_fields["task_result_id"] == ScalarSelection(value="a")
    assert request.selected_items[0].selected_fields["task_result_id"].model_dump(mode="json") == {
        "kind": "scalar",
        "value": "a",
    }
    assert request.messages[0]["role"] == "user"
    assert request.messages[0]["content"] == [
        {"type": "text", "text": "Selected items:"},
        {"type": "text", "text": "item[0].task_result_id: a"},
        {"type": "text", "text": "item[0].title: Alpha"},
        {"type": "text", "text": "item[0].image:"},
        {"type": "image_url", "image_url": {"url": "https://example.com/a.png"}},
    ]


def test_multi_openrouter_payload_creator_preserves_image_field_insertion_order(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    creator = MultiOpenRouterPayloadCreator[PromptItem](
        "openrouter-payload-creator",
        item_selector=lambda item: {
            "task_result_id": _scalar_selection(item.item_id),
            "original_image_url": _image_selection(item.image_url),
            "generated_image_url": _image_selection(f"{item.image_url}?generated=1"),
        },
    )
    setup = transform_actor_test_setup_factory(creator)

    with setup.running():
        setup.send(input_payload=(PromptItem(item_id="a", title="Alpha", image_url="https://example.com/a.png"),))
        wait_until(lambda: len(setup.processed_collector.received_events) == 1)

    request = setup.processed_collector.received_events[0].payload
    image_blocks = [block for block in request.messages[0]["content"] if block["type"] == "image_url"]
    image_urls = [cast(dict[str, str], block["image_url"])["url"] for block in image_blocks]

    assert image_urls == [
        "https://example.com/a.png",
        "https://example.com/a.png?generated=1",
    ]


def test_multi_openrouter_payload_creator_renders_image_url_selection(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    ctx_id, setup = _run_payload_creator(
        transform_actor_test_setup_factory,
        item_selector=lambda item: {
            "image": _image_selection(item.image_url),
        },
    )

    assert len(setup.error_collector.received_events) == 0
    event = setup.processed_collector.received_events[0]
    request = event.payload

    assert event.ctx_id == ctx_id
    assert request.messages[0]["content"] == [
        {"type": "text", "text": "Selected items:"},
        {"type": "text", "text": "item[0].image:"},
        {"type": "image_url", "image_url": {"url": "https://example.com/a.png"}},
    ]


def test_multi_openrouter_payload_creator_renders_file_selection(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    ctx_id, setup = _run_payload_creator(
        transform_actor_test_setup_factory,
        item_selector=lambda _item: {
            "attachment": _file_selection(
                filename="notes.txt",
                file_data="data:text/plain;base64,SGVsbG8=",
            ),
        },
    )

    assert len(setup.error_collector.received_events) == 0
    event = setup.processed_collector.received_events[0]
    request = event.payload

    assert event.ctx_id == ctx_id
    assert request.messages[0]["content"] == [
        {"type": "text", "text": "Selected items:"},
        {"type": "text", "text": "item[0].attachment:"},
        {
            "type": "file",
            "file": {
                "filename": "notes.txt",
                "file_data": "data:text/plain;base64,SGVsbG8=",
            },
        },
    ]


def test_multi_openrouter_payload_creator_renders_input_audio_selection(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    ctx_id, setup = _run_payload_creator(
        transform_actor_test_setup_factory,
        item_selector=lambda _item: {
            "audio": _input_audio_selection(data="UklGRg==", format="wav"),
        },
    )

    assert len(setup.error_collector.received_events) == 0
    event = setup.processed_collector.received_events[0]
    request = event.payload

    assert event.ctx_id == ctx_id
    assert request.messages[0]["content"] == [
        {"type": "text", "text": "Selected items:"},
        {"type": "text", "text": "item[0].audio:"},
        {
            "type": "input_audio",
            "input_audio": {
                "data": "UklGRg==",
                "format": "wav",
            },
        },
    ]


def test_multi_openrouter_payload_creator_renders_video_url_selection(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    ctx_id, setup = _run_payload_creator(
        transform_actor_test_setup_factory,
        item_selector=lambda _item: {
            "video": _video_url_selection("https://example.com/demo.mp4"),
        },
    )

    assert len(setup.error_collector.received_events) == 0
    event = setup.processed_collector.received_events[0]
    request = event.payload

    assert event.ctx_id == ctx_id
    assert request.messages[0]["content"] == [
        {"type": "text", "text": "Selected items:"},
        {"type": "text", "text": "item[0].video:"},
        {"type": "video_url", "video_url": {"url": "https://example.com/demo.mp4"}},
    ]


def test_multi_openrouter_payload_creator_renders_scalar_only_item_as_text(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    ctx_id, setup = _run_payload_creator(
        transform_actor_test_setup_factory,
        item_selector=lambda item: {
            "task_result_id": _scalar_selection(item.item_id),
            "title": _scalar_selection(item.title),
            "is_featured": _scalar_selection(False),
        },
    )

    assert len(setup.error_collector.received_events) == 0
    event = setup.processed_collector.received_events[0]
    request = event.payload

    assert event.ctx_id == ctx_id
    assert request.messages[0]["content"] == [
        {"type": "text", "text": "Selected items:"},
        {"type": "text", "text": "item[0].task_result_id: a"},
        {"type": "text", "text": "item[0].title: Alpha"},
        {"type": "text", "text": "item[0].is_featured: False"},
    ]
    assert all(block["type"] == "text" for block in request.messages[0]["content"])


def test_multi_openrouter_payload_creator_preserves_insertion_order_across_mixed_modalities(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    ctx_id, setup = _run_payload_creator(
        transform_actor_test_setup_factory,
        item_selector=lambda item: {
            "title": _scalar_selection(item.title),
            "image": _image_selection(item.image_url),
            "attachment": _file_selection(
                filename="notes.txt",
                file_data="data:text/plain;base64,SGVsbG8=",
            ),
            "audio": _input_audio_selection(data="UklGRg==", format="wav"),
            "video": _video_url_selection("https://example.com/demo.mp4"),
            "task_result_id": _scalar_selection(item.item_id),
        },
    )

    assert len(setup.error_collector.received_events) == 0
    event = setup.processed_collector.received_events[0]
    request = event.payload

    assert event.ctx_id == ctx_id
    assert request.messages[0]["content"] == [
        {"type": "text", "text": "Selected items:"},
        {"type": "text", "text": "item[0].title: Alpha"},
        {"type": "text", "text": "item[0].image:"},
        {"type": "image_url", "image_url": {"url": "https://example.com/a.png"}},
        {"type": "text", "text": "item[0].attachment:"},
        {
            "type": "file",
            "file": {
                "filename": "notes.txt",
                "file_data": "data:text/plain;base64,SGVsbG8=",
            },
        },
        {"type": "text", "text": "item[0].audio:"},
        {
            "type": "input_audio",
            "input_audio": {
                "data": "UklGRg==",
                "format": "wav",
            },
        },
        {"type": "text", "text": "item[0].video:"},
        {"type": "video_url", "video_url": {"url": "https://example.com/demo.mp4"}},
        {"type": "text", "text": "item[0].task_result_id: a"},
    ]


def test_multi_openrouter_payload_creator_rejects_unsupported_selection_object(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    ctx_id, setup = _run_payload_creator(
        transform_actor_test_setup_factory,
        item_selector=lambda _item: {
            "unsupported": _UnsupportedSelectionObject(),
        },
    )

    assert len(setup.processed_collector.received_events) == 0
    error_event = setup.error_collector.received_events[0]
    error_payload = error_event.payload

    assert error_event.ctx_id == ctx_id
    assert isinstance(error_payload, SafeInvokeWrappedException)
    assert error_payload.__cause__ is not None
    assert isinstance(error_payload.__cause__, ValueError)
    assert "unsupported" in str(error_payload.__cause__).lower()


def test_multi_openrouter_payload_creator_rejects_malformed_multimodal_selection_dict(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    ctx_id, setup = _run_payload_creator(
        transform_actor_test_setup_factory,
        item_selector=lambda _item: {
            "attachment": {
                "kind": "file",
                "filename": "notes.txt",
            },
        },
    )

    assert len(setup.processed_collector.received_events) == 0
    error_event = setup.error_collector.received_events[0]
    error_payload = error_event.payload

    assert error_event.ctx_id == ctx_id
    assert isinstance(error_payload, SafeInvokeWrappedException)
    assert error_payload.__cause__ is not None
    assert isinstance(error_payload.__cause__, ValueError)
    assert "attachment" in str(error_payload.__cause__)
    assert "malformed" in str(error_payload.__cause__).lower()


def test_multi_openrouter_payload_creator_rejects_multimodal_selection_dict_with_extra_key(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    ctx_id, setup = _run_payload_creator(
        transform_actor_test_setup_factory,
        item_selector=lambda _item: {
            "attachment": {
                "kind": "file",
                "filename": "notes.txt",
                "file_data": "data:text/plain;base64,SGVsbG8=",
                "media_type": "text/plain",
            },
        },
    )

    assert len(setup.processed_collector.received_events) == 0
    error_event = setup.error_collector.received_events[0]
    error_payload = error_event.payload

    assert error_event.ctx_id == ctx_id
    assert isinstance(error_payload, SafeInvokeWrappedException)
    assert error_payload.__cause__ is not None
    assert isinstance(error_payload.__cause__, ValueError)
    assert "attachment" in str(error_payload.__cause__)
    assert "malformed" in str(error_payload.__cause__).lower()


def test_openrouter_inference_request_rejects_malformed_rendered_message_content_block() -> None:
    with pytest.raises(ValidationError, match="content"):
        OpenRouterInferenceRequest.model_validate(
            {
                "selected_items": [],
                "messages": [
                    {
                        "role": "user",
                        "content": [{"bogus": "x"}],
                    }
                ],
            }
        )


def test_select_single_task_result_fields_extracts_supported_scalar_fields() -> None:
    task_result_id = UUID("95843dde-5f4d-4204-b3ae-d24ed6be4ffc")
    selector = select_single_task_result_fields(
        include_task_result_id=True,
        include_target_hotkey=True,
    )

    task_result = SingleTaskResult[dict[str, str], dict[str, str], dict[str, str]](
        id=TaskResultId(task_result_id),
        result=build_nexus_task_result(
            executor_payload={"payload": "x"},
            output={"output": "ignored"},
            block_number=123,
            target_hotkey="hk1",
        ),
        executor_public_output={"public": "y"},
    )

    selected_fields = selector(task_result)

    assert selected_fields == {
        "task_result_id": ScalarSelection(value=str(task_result_id)),
        "target_hotkey": ScalarSelection(value="hk1"),
    }


@pytest.mark.parametrize(
    "unsupported_flag",
    [
        "include_executor_payload",
        "include_executor_output",
        "include_executor_public_output",
    ],
)
def test_select_single_task_result_fields_rejects_unsupported_executor_flags(unsupported_flag: str) -> None:
    with pytest.raises(ValueError, match=unsupported_flag):
        select_single_task_result_fields(**{unsupported_flag: True})
