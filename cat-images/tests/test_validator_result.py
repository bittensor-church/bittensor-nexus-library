import pytest
from pydantic import ValidationError

from cat_images.subnet_models import ImageHash, S3Url, ValidatorResult


def test_validator_result_requires_image_hash() -> None:
    with pytest.raises(ValidationError):
        ValidatorResult.model_validate(
            {
                "result_image_url": "https://example.com/result.png",
            }
        )


def test_validator_result_parses_validator_payload_shape() -> None:
    parsed = ValidatorResult.model_validate(
        {
            "input": {"image_hash": "abc123"},
            "presigned_url": "https://example.com/presigned-result.png",
        }
    )

    assert parsed.result_image_url == S3Url("https://example.com/presigned-result.png")
    assert parsed.image_hash == ImageHash("abc123")
