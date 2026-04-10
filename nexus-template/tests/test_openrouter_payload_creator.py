# pyright: basic

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID

import pytest
from pydantic import BaseModel, ValidationError
from transform_test_utils import TransformActorTestSetupFactory
from utils import build_neuron, dummy_block_beat, wait_until

from nexus.actors.openrouter_payload_creator import MultiOpenRouterPayloadCreator, OpenRouterInferenceRequest
from nexus.actors.openrouter_selection import (
    FileField,
    ImageUrlField,
    InputAudioField,
    ScalarField,
    VideoUrlField,
)
from nexus.core.runtime.nexus_task_types import TaskResultId
from nexus.core.runtime.task_result_store import SuccessfulTaskResult
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


def _build_successful_task_result(
    *,
    task_result_id: UUID,
    executor_payload: dict[str, str],
    executor_output: dict[str, str],
    executor_public_output: dict[str, str],
    target_hotkey: str,
) -> SuccessfulTaskResult[dict[str, str], dict[str, str], dict[str, str]]:
    processing_started = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    return SuccessfulTaskResult(
        id=TaskResultId(task_result_id),
        processing_started=processing_started,
        processing_finished=processing_started + timedelta(seconds=1),
        block_at_finish=dummy_block_beat(123),
        executor_payload=executor_payload,
        target=build_neuron(uid=1, hotkey=target_hotkey, validator_permit=False),
        executor_output=executor_output,
        executor_public_output=executor_public_output,
    )


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
            "task_result_id": ScalarField(value=item.item_id),
            "title": ScalarField(value=item.title),
            "image": ImageUrlField(url=item.image_url),
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
    assert request.fields[0].fields["task_result_id"] == ScalarField(value="a")
    assert request.fields[0].fields["task_result_id"].model_dump(mode="json") == {
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
            "task_result_id": ScalarField(value=item.item_id),
            "original_image_url": ImageUrlField(url=item.image_url),
            "generated_image_url": ImageUrlField(url=f"{item.image_url}?generated=1"),
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
            "image": ImageUrlField(url=item.image_url),
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
            "attachment": FileField(
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
            "audio": InputAudioField(data="UklGRg==", format="wav"),
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
            "video": VideoUrlField(url="https://example.com/demo.mp4"),
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
            "task_result_id": ScalarField(value=item.item_id),
            "title": ScalarField(value=item.title),
            "is_featured": ScalarField(value=False),
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
            "title": ScalarField(value=item.title),
            "image": ImageUrlField(url=item.image_url),
            "attachment": FileField(
                filename="notes.txt",
                file_data="data:text/plain;base64,SGVsbG8=",
            ),
            "audio": InputAudioField(data="UklGRg==", format="wav"),
            "video": VideoUrlField(url="https://example.com/demo.mp4"),
            "task_result_id": ScalarField(value=item.item_id),
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
    assert "field 'attachment'" in str(error_payload.__cause__)
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
    assert "field 'attachment'" in str(error_payload.__cause__)
    assert "malformed" in str(error_payload.__cause__).lower()


def test_multi_openrouter_payload_creator_skips_items_with_no_selected_fields(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    creator = MultiOpenRouterPayloadCreator[PromptItem](
        "openrouter-payload-creator",
        item_selector=lambda item: None
        if item.item_id == "skip"
        else {
            "task_result_id": ScalarField(value=item.item_id),
            "title": ScalarField(value=item.title),
        },
    )
    setup = transform_actor_test_setup_factory(creator)

    with setup.running():
        setup.send(
            input_payload=(
                PromptItem(item_id="skip", title="Skipped", image_url="https://example.com/skipped.png"),
                PromptItem(item_id="keep", title="Kept", image_url="https://example.com/kept.png"),
            )
        )
        wait_until(lambda: len(setup.processed_collector.received_events) == 1)

    request = setup.processed_collector.received_events[0].payload

    assert len(request.fields) == 1
    assert request.fields[0].fields == {
        "task_result_id": ScalarField(value="keep"),
        "title": ScalarField(value="Kept"),
    }
    assert request.messages[0]["content"] == [
        {"type": "text", "text": "Selected items:"},
        {"type": "text", "text": "item[0].task_result_id: keep"},
        {"type": "text", "text": "item[0].title: Kept"},
    ]


def test_multi_openrouter_payload_creator_rejects_empty_selected_item_batches(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    ctx_id, setup = _run_payload_creator(
        transform_actor_test_setup_factory,
        item_selector=lambda _item: None,
    )

    assert len(setup.processed_collector.received_events) == 0
    error_event = setup.error_collector.received_events[0]
    error_payload = error_event.payload

    assert error_event.ctx_id == ctx_id
    assert isinstance(error_payload, SafeInvokeWrappedException)
    assert error_payload.__cause__ is not None
    assert isinstance(error_payload.__cause__, ValueError)
    assert "selected item" in str(error_payload.__cause__).lower()


def test_openrouter_inference_request_rejects_malformed_rendered_message_content_block() -> None:
    with pytest.raises(ValidationError, match="content"):
        OpenRouterInferenceRequest.model_validate(
            {
                "fields": [],
                "messages": [
                    {
                        "role": "user",
                        "content": [{"bogus": "x"}],
                    }
                ],
            }
        )


def test_inline_task_result_selector_extracts_supported_scalar_fields() -> None:
    task_result_id = UUID("95843dde-5f4d-4204-b3ae-d24ed6be4ffc")
    selector = lambda task_result: {
        "task_result_id": ScalarField(value=str(task_result.id)),
        "target_hotkey": ScalarField(value=task_result.target.hotkey),
    }

    task_result = _build_successful_task_result(
        task_result_id=task_result_id,
        executor_payload={"payload": "x"},
        executor_output={"output": "ignored"},
        executor_public_output={"public": "y"},
        target_hotkey="hk1",
    )

    selected_fields = selector(task_result)

    assert selected_fields == {
        "task_result_id": ScalarField(value=str(task_result_id)),
        "target_hotkey": ScalarField(value="hk1"),
    }


def test_inline_task_result_selector_extracts_mixed_openrouter_fields_in_order() -> None:
    task_result_id = UUID("95843dde-5f4d-4204-b3ae-d24ed6be4ffc")
    selector = lambda task_result: {
        "source_image": ImageUrlField(url=str(task_result.executor_payload["image_url"])),
        "task_result_id": ScalarField(value=str(task_result.id)),
        "public_caption": ScalarField(
            value=task_result.executor_public_output["caption"]
            if task_result.executor_public_output is not None
            else None
        ),
    }

    task_result = _build_successful_task_result(
        task_result_id=task_result_id,
        executor_payload={"image_url": "https://example.com/source.png"},
        executor_output={"output": "ignored"},
        executor_public_output={"caption": "Cat on a chair"},
        target_hotkey="hk1",
    )

    selected_fields = selector(task_result)

    assert list(selected_fields) == ["source_image", "task_result_id", "public_caption"]
    assert selected_fields == {
        "source_image": ImageUrlField(url="https://example.com/source.png"),
        "task_result_id": ScalarField(value=str(task_result_id)),
        "public_caption": ScalarField(value="Cat on a chair"),
    }
