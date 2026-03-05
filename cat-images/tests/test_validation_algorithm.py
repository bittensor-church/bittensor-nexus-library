import functools
import json
import uuid
from typing import Any

import httpx
import pytest
from nexus.actors.executor_communicator.embedded_executor_communicator import EmbeddedExecutorCommunicator
from nexus.actors.payload_creator import S3PresignedUrl, WithPresignedUrl
from nexus.actors.task_input_output_creator import BatchedTaskInputOutput, TaskInputOutput
from nexus.core.runtime.nexus_task_types import TaskResultId
from nexus.utils.types import NetUid
from pydantic import ValidationError

from cat_images import validation_algorithm
from cat_images.subnet import ImageHash, ImageName, MinerResult, S3Url, SingleCatImageInput, ValidationResult
from cat_images.validator import Validator
from cat_images.validator_settings import CatValidatorSettings, clear_validator_settings_cache


@pytest.fixture(autouse=True)
def reset_validator_settings_state() -> None:
    clear_validator_settings_cache()


def _build_test_batch() -> BatchedTaskInputOutput[
    WithPresignedUrl[SingleCatImageInput],
    MinerResult,
    WithPresignedUrl[MinerResult],
]:
    task_1_id = TaskResultId(uuid.UUID("00000000-0000-0000-0000-000000000001"))
    task_2_id = TaskResultId(uuid.UUID("00000000-0000-0000-0000-000000000002"))
    task_1 = TaskInputOutput(
        task_result_id=task_1_id,
        task_input=WithPresignedUrl(
            input=SingleCatImageInput(
                image_s3_url=S3Url("https://example.com/source-1.png"),
                image_name=ImageName("source-1.png"),
            ),
            presigned_url=S3PresignedUrl("https://example.com/upload-1.png"),
        ),
        task_output=MinerResult(image_hash=ImageHash("hash-1")),
        task_public_output=WithPresignedUrl(
            input=MinerResult(image_hash=ImageHash("hash-1")),
            presigned_url=S3PresignedUrl("https://example.com/generated-1.png"),
        ),
    )
    task_2 = TaskInputOutput(
        task_result_id=task_2_id,
        task_input=WithPresignedUrl(
            input=SingleCatImageInput(
                image_s3_url=S3Url("https://example.com/source-2.png"),
                image_name=ImageName("source-2.png"),
            ),
            presigned_url=S3PresignedUrl("https://example.com/upload-2.png"),
        ),
        task_output=MinerResult(image_hash=ImageHash("hash-2")),
        task_public_output=WithPresignedUrl(
            input=MinerResult(image_hash=ImageHash("hash-2")),
            presigned_url=S3PresignedUrl("https://example.com/generated-2.png"),
        ),
    )
    return BatchedTaskInputOutput(task_input_outputs=(task_1, task_2))


class _FakeResponse:
    def __init__(self, payload: Any, *, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code < 400:
            return
        request = httpx.Request(method="POST", url="https://openrouter.local/test")
        response = httpx.Response(status_code=self.status_code, request=request)
        raise httpx.HTTPStatusError("openrouter request failed", request=request, response=response)

    def json(self) -> Any:
        return self._payload


class _FakeClient:
    def __init__(self, *, record: dict[str, Any], response_payload: Any) -> None:
        self._record = record
        self._response_payload = response_payload

    def __enter__(self) -> _FakeClient:
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        return None

    def post(self, url: str, *, json: dict[str, Any], headers: dict[str, str]) -> _FakeResponse:
        self._record["url"] = url
        self._record["json"] = json
        self._record["headers"] = headers
        self._record["post_calls"] = self._record.get("post_calls", 0) + 1
        return _FakeResponse(self._response_payload)


def _patch_openrouter_client(
    monkeypatch: pytest.MonkeyPatch,
    *,
    response_payload: Any,
) -> dict[str, Any]:
    record: dict[str, Any] = {}

    def fake_client_factory(*_: Any, **kwargs: Any) -> _FakeClient:
        record["client_kwargs"] = kwargs
        return _FakeClient(record=record, response_payload=response_payload)

    monkeypatch.setattr(validation_algorithm.httpx, "Client", fake_client_factory)
    return record


def _make_test_settings() -> CatValidatorSettings:
    return CatValidatorSettings(
        netuid=NetUid(278),
        openrouter_api_key="test-openrouter-key",
        openrouter_url="https://openrouter.local/chat/completions",
        openrouter_model="test-model",
        validation_prompt="Test prompt for validation.",
        validation_openrouter_timeout_seconds=33.0,
        validation_openrouter_temperature=0.0,
        external_ip="127.0.0.1",
        pylon_service_address="http://127.0.0.1:18000",
        pylon_open_access_token="token",
    )


def _validate_batch(
    batch: BatchedTaskInputOutput[WithPresignedUrl[SingleCatImageInput], MinerResult, WithPresignedUrl[MinerResult]],
    settings: CatValidatorSettings,
) -> BatchedTaskInputOutput[WithPresignedUrl[SingleCatImageInput], ValidationResult, ValidationResult]:
    return validation_algorithm.validate(
        batch,
        settings=settings,
    )


def test_validate_successfully_scores_each_task(monkeypatch: pytest.MonkeyPatch) -> None:
    batch = _build_test_batch()
    settings = _make_test_settings()
    first_task_id = str(batch.task_input_outputs[0].task_result_id)
    second_task_id = str(batch.task_input_outputs[1].task_result_id)
    openrouter_response = {
        "choices": [
            {
                "message": {
                    "content": json.dumps(
                        {
                            "scores_by_task_result_id": {
                                first_task_id: 87,
                                second_task_id: 42,
                            }
                        }
                    )
                }
            }
        ]
    }
    record = _patch_openrouter_client(monkeypatch, response_payload=openrouter_response)

    validated = _validate_batch(batch, settings)

    assert len(validated.task_input_outputs) == 2
    assert validated.task_input_outputs[0].task_result_id == batch.task_input_outputs[0].task_result_id
    assert validated.task_input_outputs[1].task_result_id == batch.task_input_outputs[1].task_result_id
    assert validated.task_input_outputs[0].task_output.score == 87
    assert validated.task_input_outputs[1].task_output.score == 42
    assert validated.task_input_outputs[0].task_public_output.score == 87
    assert validated.task_input_outputs[1].task_public_output.score == 42
    assert record["url"] == settings.openrouter_url
    assert record["headers"]["Authorization"] == f"Bearer {settings.openrouter_api_key}"
    assert record["client_kwargs"]["timeout"] == settings.validation_openrouter_timeout_seconds

    content = record["json"]["messages"][0]["content"]
    image_urls = [item["image_url"]["url"] for item in content if item.get("type") == "image_url"]
    assert image_urls == [
        "https://example.com/source-1.png",
        "https://example.com/generated-1.png",
        "https://example.com/source-2.png",
        "https://example.com/generated-2.png",
    ]


def test_validate_empty_batch_skips_openrouter_call(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _make_test_settings()
    empty_batch: BatchedTaskInputOutput[
        WithPresignedUrl[SingleCatImageInput], MinerResult, WithPresignedUrl[MinerResult]
    ]
    empty_batch = BatchedTaskInputOutput(task_input_outputs=())
    record = _patch_openrouter_client(monkeypatch, response_payload={})

    validated = _validate_batch(empty_batch, settings)

    assert validated.task_input_outputs == ()
    assert record.get("post_calls", 0) == 0


def test_validate_raises_for_missing_task_id(monkeypatch: pytest.MonkeyPatch) -> None:
    batch = _build_test_batch()
    settings = _make_test_settings()
    only_first_task_id = str(batch.task_input_outputs[0].task_result_id)
    record = _patch_openrouter_client(
        monkeypatch,
        response_payload={
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "scores_by_task_result_id": {
                                    only_first_task_id: 50,
                                }
                            }
                        )
                    }
                }
            ]
        },
    )

    with pytest.raises(ValueError, match="task ids mismatch"):
        _validate_batch(batch, settings)

    assert record.get("post_calls", 0) == 1


def test_validate_raises_for_unexpected_task_id(monkeypatch: pytest.MonkeyPatch) -> None:
    batch = _build_test_batch()
    settings = _make_test_settings()
    first_task_id = str(batch.task_input_outputs[0].task_result_id)
    second_task_id = str(batch.task_input_outputs[1].task_result_id)
    record = _patch_openrouter_client(
        monkeypatch,
        response_payload={
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "scores_by_task_result_id": {
                                    first_task_id: 10,
                                    second_task_id: 20,
                                    "00000000-0000-0000-0000-000000000099": 30,
                                }
                            }
                        )
                    }
                }
            ]
        },
    )

    with pytest.raises(ValueError, match="task ids mismatch"):
        _validate_batch(batch, settings)

    assert record.get("post_calls", 0) == 1


def test_validate_raises_for_out_of_range_score(monkeypatch: pytest.MonkeyPatch) -> None:
    batch = _build_test_batch()
    settings = _make_test_settings()
    first_task_id = str(batch.task_input_outputs[0].task_result_id)
    second_task_id = str(batch.task_input_outputs[1].task_result_id)
    _patch_openrouter_client(
        monkeypatch,
        response_payload={
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "scores_by_task_result_id": {
                                    first_task_id: 101,
                                    second_task_id: 20,
                                }
                            }
                        )
                    }
                }
            ]
        },
    )

    with pytest.raises(ValidationError, match="must be in range \\[1, 100\\]"):
        _validate_batch(batch, settings)


def test_validate_raises_for_non_json_content(monkeypatch: pytest.MonkeyPatch) -> None:
    batch = _build_test_batch()
    settings = _make_test_settings()
    _patch_openrouter_client(
        monkeypatch,
        response_payload={
            "choices": [
                {
                    "message": {
                        "content": "this is not json",
                    }
                }
            ]
        },
    )

    with pytest.raises(json.JSONDecodeError):
        _validate_batch(batch, settings)


def test_validator_binds_settings_to_validation_executor() -> None:
    settings = CatValidatorSettings(
        netuid=NetUid(278),
        openrouter_api_key="validator-key",
        openrouter_url="https://openrouter.local/custom",
        openrouter_model="validator-model",
        validation_prompt="Custom validation prompt",
        validation_openrouter_timeout_seconds=11.0,
        validation_openrouter_temperature=0.2,
        external_ip="127.0.0.1",
        pylon_service_address="http://127.0.0.1:18000",
        pylon_open_access_token="token",
    )

    validator = Validator(settings)
    communicator = validator.validation_task.executor_communicator
    assert isinstance(communicator, EmbeddedExecutorCommunicator)
    executor_func = communicator.executor_func
    assert isinstance(executor_func, functools.partial)
    assert executor_func.func is validation_algorithm.validate
    assert executor_func.keywords is not None
    assert executor_func.keywords["settings"] is settings
