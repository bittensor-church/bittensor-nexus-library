from typing import Any

import httpx
import pytest

from cat_images.facilitator.models import RegisteredValidator
from cat_images.facilitator.submitter import JobSubmitter
from cat_images.facilitator.types import ValidatorHotkey
from cat_images.subnet import ImageHash, S3Url, SingleCatImageInput


class _FakeResponse:
    def __init__(self, payload: Any, *, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code < 400:
            return
        request = httpx.Request(method="POST", url="http://validator.local/cat-images")
        response = httpx.Response(status_code=self.status_code, request=request)
        raise httpx.HTTPStatusError("validator request failed", request=request, response=response)

    def json(self) -> Any:
        return self._payload


class _FakeClient:
    def __init__(self, record: dict[str, Any], response_payload: Any) -> None:
        self._record = record
        self._response_payload = response_payload

    def __enter__(self) -> _FakeClient:
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        return None

    def post(self, url: str, *, json: dict[str, Any]) -> _FakeResponse:
        self._record["url"] = url
        self._record["json"] = json
        return _FakeResponse(self._response_payload)


def _validator() -> RegisteredValidator:
    return RegisteredValidator(
        hotkey=ValidatorHotkey("validator-1"),
        job_submission_url="http://validator.local/cat-images",
    )


def _job_spec() -> SingleCatImageInput:
    return SingleCatImageInput(image_s3_url=S3Url("https://example.com/source.png"))


def test_submitter_parses_real_validator_payload_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    record: dict[str, Any] = {}

    def fake_client_factory(*_: Any, **__: Any) -> _FakeClient:
        return _FakeClient(
            record=record,
            response_payload={
                "input": {"image_hash": "deadbeef"},
                "presigned_url": "https://example.com/result-presigned.png",
            },
        )

    monkeypatch.setattr("cat_images.facilitator.submitter.httpx.Client", fake_client_factory)

    submitter = JobSubmitter(max_retries=1, timeout=1.0)
    result = submitter.submit(_validator(), _job_spec())

    assert record["url"] == "http://validator.local/cat-images"
    assert record["json"] == {"image_s3_url": "https://example.com/source.png"}
    assert result.result_image_url == S3Url("https://example.com/result-presigned.png")
    assert result.image_hash == ImageHash("deadbeef")


def test_submitter_parses_legacy_validator_payload_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_client_factory(*_: Any, **__: Any) -> _FakeClient:
        return _FakeClient(
            record={},
            response_payload={
                "result_image_url": "https://example.com/legacy-result.png",
            },
        )

    monkeypatch.setattr("cat_images.facilitator.submitter.httpx.Client", fake_client_factory)

    submitter = JobSubmitter(max_retries=1, timeout=1.0)
    result = submitter.submit(_validator(), _job_spec())

    assert result.result_image_url == S3Url("https://example.com/legacy-result.png")
    assert result.image_hash is None
