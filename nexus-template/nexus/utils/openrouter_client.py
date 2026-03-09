"""Helpers for calling OpenRouter chat completions that return JSON content."""

from collections.abc import Mapping
from typing import Any, cast

import httpx
from pydantic import BaseModel
from pydantic_settings import BaseSettings

_RETRY_TRANSPORT = httpx.HTTPTransport(retries=3)


class OpenRouterConfigurationError(ValueError):
    """Raised when a BaseSettings object does not expose valid OpenRouter configuration."""


def _find_setting(settings_data: Mapping[str, object], *field_names: str) -> tuple[str, object]:
    for field_name in field_names:
        if field_name in settings_data:
            return field_name, settings_data[field_name]
    joined_names = ", ".join(field_names)
    raise OpenRouterConfigurationError(f"OpenRouter settings missing required field(s): {joined_names}")


def _require_str_setting(settings_data: Mapping[str, object], *field_names: str) -> str:
    field_name, value = _find_setting(settings_data, *field_names)
    if not isinstance(value, str) or len(value.strip()) == 0:
        raise OpenRouterConfigurationError(
            f"OpenRouter setting {field_name} must be a non-empty string, got {type(value).__name__}"
        )
    return value


def _require_float_setting(settings_data: Mapping[str, object], *field_names: str) -> float:
    field_name, value = _find_setting(settings_data, *field_names)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise OpenRouterConfigurationError(
            f"OpenRouter setting {field_name} must be numeric, got {type(value).__name__}"
        )
    return float(value)


def _invalid_response_error(reason: str, raw_response: object) -> ValueError:
    return ValueError(
        f"Failed to extract OpenRouter choices[0].message.content: {reason}; raw_response={raw_response!r}"
    )


def _extract_textual_message_content(raw_response: dict[str, object]) -> str:
    choices_obj = raw_response.get("choices")
    if not isinstance(choices_obj, list):
        raise _invalid_response_error("choices is not a list", raw_response)
    choices = cast(list[object], choices_obj)
    if len(choices) == 0:
        raise _invalid_response_error("choices is empty", raw_response)

    first_choice_obj = choices[0]
    if not isinstance(first_choice_obj, dict):
        raise _invalid_response_error("choices[0] is not a dict", raw_response)
    first_choice = cast(dict[str, object], first_choice_obj)

    message_obj = first_choice.get("message")
    if not isinstance(message_obj, dict):
        raise _invalid_response_error("choices[0].message is not a dict", raw_response)
    message = cast(dict[str, object], message_obj)

    message_content = message.get("content")
    if not isinstance(message_content, str):
        raise _invalid_response_error("choices[0].message.content is not a JSON string", raw_response)
    return message_content


def query[ResponseModelT: BaseModel](
    *,
    messages: list[dict[str, Any]],
    settings: BaseSettings,
    response_model: type[ResponseModelT],
) -> ResponseModelT:
    """Send a chat completion request to OpenRouter and validate the JSON reply.

    Raises:
        OpenRouterConfigurationError: The provided settings object is missing a required OpenRouter field.
        ValueError: The OpenRouter response envelope is malformed.
        ValidationError: The OpenRouter message content is invalid JSON or does not match ``response_model``.
    """
    settings_data = cast(dict[str, object], settings.model_dump())

    payload = {
        "model": _require_str_setting(settings_data, "openrouter_model"),
        "temperature": _require_float_setting(
            settings_data,
            "validation_openrouter_temperature",
            "openrouter_temperature",
        ),
        "messages": messages,
    }

    with httpx.Client(
        transport=_RETRY_TRANSPORT,
        timeout=_require_float_setting(
            settings_data,
            "validation_openrouter_timeout_seconds",
            "openrouter_timeout_seconds",
        ),
    ) as client:
        response = client.post(
            _require_str_setting(settings_data, "openrouter_url"),
            json=payload,
            headers={"Authorization": f"Bearer {_require_str_setting(settings_data, 'openrouter_api_key')}"},
        )
        response.raise_for_status()

    raw_response_obj = response.json()
    if not isinstance(raw_response_obj, dict):
        raise ValueError("OpenRouter response must be a JSON object")
    raw_response = cast(dict[str, object], raw_response_obj)

    message_content = _extract_textual_message_content(raw_response).strip()
    return response_model.model_validate_json(message_content)
