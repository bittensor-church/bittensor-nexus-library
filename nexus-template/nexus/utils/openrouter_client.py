"""OpenRouter client for chat completions that return structured JSON content."""

from typing import Any, cast

import httpx
from pydantic import BaseModel

from nexus.utils.openrouter_config import OpenRouterSettingsMixin

_RETRY_TRANSPORT = httpx.HTTPTransport(retries=3)


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


class OpenRouterClient:
    """Synchronous OpenRouter client bound to a concrete validator configuration."""

    def __init__(
        self,
        *,
        url: str,
        api_key: str,
        model: str,
        timeout_seconds: float,
        temperature: float,
    ) -> None:
        self._url = url
        self._api_key = api_key
        self._model = model
        self._timeout_seconds = timeout_seconds
        self._temperature = temperature

    @classmethod
    def from_settings(cls, settings: OpenRouterSettingsMixin) -> OpenRouterClient:
        return cls(
            url=settings.openrouter_url,
            api_key=settings.openrouter_api_key,
            model=settings.openrouter_model,
            timeout_seconds=settings.validation_openrouter_timeout_seconds,
            temperature=settings.validation_openrouter_temperature,
        )

    def query[ResponseModelT: BaseModel](
        self,
        *,
        messages: list[dict[str, Any]],
        response_model: type[ResponseModelT],
    ) -> ResponseModelT:
        """Send a chat completion request to OpenRouter and validate the JSON reply.

        Raises:
            ValueError: The OpenRouter response envelope is malformed.
            ValidationError: The OpenRouter message content is invalid JSON or does not match ``response_model``.
        """
        payload = {
            "model": self._model,
            "temperature": self._temperature,
            "messages": messages,
        }

        with httpx.Client(
            transport=_RETRY_TRANSPORT,
            timeout=self._timeout_seconds,
        ) as client:
            response = client.post(
                self._url,
                json=payload,
                headers={"Authorization": f"Bearer {self._api_key}"},
            )
            response.raise_for_status()

        raw_response_obj = response.json()
        if not isinstance(raw_response_obj, dict):
            raise ValueError("OpenRouter response must be a JSON object")
        raw_response = cast(dict[str, object], raw_response_obj)

        message_content = _extract_textual_message_content(raw_response).strip()
        return response_model.model_validate_json(message_content)


__all__ = ["OpenRouterClient"]
