import json
import logging
from collections.abc import Mapping
from typing import Any, cast

import httpx
from nexus.actors.task_input_output_creator import BatchedTaskInputOutput, TaskInputOutput
from nexus.actors.weight_setter import WeightsCalculationBundle
from nexus.utils.exceptions import NexusTaskName
from nexus.utils.types import Hotkey, Weight
from pydantic import BaseModel, Field, field_validator

from cat_images.subnet import MinerPayload, MinerPublicResult, MinerResult, ValidationResult
from cat_images.validator_settings import CatValidatorSettings

log = logging.getLogger("validation-algorithm")
_RETRY_TRANSPORT = httpx.HTTPTransport(retries=3)


class _TaskScores(BaseModel):
    scores_by_task_result_id: dict[str, int] = Field(default_factory=dict)

    @field_validator("scores_by_task_result_id")
    @classmethod
    def _validate_score_range(cls, value: dict[str, int]) -> dict[str, int]:
        for task_id, score in value.items():
            if score < 1 or score > 100:
                raise ValueError(f"Score for task_result_id={task_id} must be in range [1, 100], got {score}")
        return value


class _ValidationPair(BaseModel):
    task_result_id: str
    original_image_url: str
    generated_image_url: str


def _extract_pairs(
    batch_to_validate: BatchedTaskInputOutput[MinerPayload, MinerResult, MinerPublicResult],
) -> tuple[_ValidationPair, ...]:
    pairs: list[_ValidationPair] = []
    for task_input_output in batch_to_validate.task_input_outputs:
        task_id = str(task_input_output.task_result_id)
        pairs.append(
            _ValidationPair(
                task_result_id=task_id,
                original_image_url=str(task_input_output.task_input.input.image_s3_url),
                generated_image_url=str(task_input_output.task_public_output.presigned_url),
            )
        )
    return tuple(pairs)


def _build_openrouter_payload(
    *,
    pairs: tuple[_ValidationPair, ...],
    settings: CatValidatorSettings,
) -> dict[str, Any]:
    content: list[dict[str, Any]] = [
        {
            "type": "text",
            "text": (
                f"{settings.validation_prompt} "
                f"The task_result_ids to score are: {[pair.task_result_id for pair in pairs]}"
            ),
        },
    ]
    for pair in pairs:
        content.extend(
            (
                {
                    "type": "text",
                    "text": f"task_result_id={pair.task_result_id} original_image",
                },
                {
                    "type": "image_url",
                    "image_url": {"url": pair.original_image_url},
                },
                {
                    "type": "text",
                    "text": f"task_result_id={pair.task_result_id} generated_image",
                },
                {
                    "type": "image_url",
                    "image_url": {"url": pair.generated_image_url},
                },
            )
        )

    return {
        "model": settings.openrouter_model,
        "temperature": settings.validation_openrouter_temperature,
        "messages": [
            {
                "role": "user",
                "content": content,
            }
        ],
    }


def _extract_textual_message_content(raw_response: dict[str, object]) -> str:
    try:
        choices_obj = raw_response["choices"]
        if not isinstance(choices_obj, list):
            raise TypeError("choices is not a list")
        choices = cast(list[object], choices_obj)
        first_choice_obj = choices[0] if len(choices) > 0 else None
        if not isinstance(first_choice_obj, dict):
            raise TypeError("choices[0] is not a dict")
        first_choice = cast(dict[str, object], first_choice_obj)
        message_obj = first_choice["message"]
        if not isinstance(message_obj, dict):
            raise TypeError("choices[0].message is not a dict")
        message = cast(dict[str, object], message_obj)
        message_content = message["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError("OpenRouter response does not contain choices[0].message.content") from exc
    if not isinstance(message_content, str):
        raise ValueError("OpenRouter response choices[0].message.content must be a JSON string")
    return message_content


def _call_openrouter_for_scores(
    *,
    pairs: tuple[_ValidationPair, ...],
    settings: CatValidatorSettings,
) -> dict[str, int]:
    payload = _build_openrouter_payload(pairs=pairs, settings=settings)
    with httpx.Client(transport=_RETRY_TRANSPORT, timeout=settings.validation_openrouter_timeout_seconds) as client:
        response = client.post(
            settings.openrouter_url,
            json=payload,
            headers={"Authorization": f"Bearer {settings.openrouter_api_key}"},
        )
        response.raise_for_status()
    raw_response_obj = response.json()
    if not isinstance(raw_response_obj, dict):
        raise ValueError("OpenRouter response must be a JSON object")
    raw_response = cast(dict[str, object], raw_response_obj)

    message_content = _extract_textual_message_content(raw_response).strip()
    parsed_message = json.loads(message_content)
    scored = _TaskScores.model_validate(parsed_message)
    return scored.scores_by_task_result_id


def _validate_scores_cover_all_tasks(
    *,
    pairs: tuple[_ValidationPair, ...],
    scores_by_task_id: dict[str, int],
) -> None:
    expected_task_ids = {pair.task_result_id for pair in pairs}
    actual_task_ids = set(scores_by_task_id.keys())
    if expected_task_ids != actual_task_ids:
        missing_task_ids = sorted(expected_task_ids - actual_task_ids)
        unexpected_task_ids = sorted(actual_task_ids - expected_task_ids)
        raise ValueError(
            "OpenRouter validation output task ids mismatch; "
            f"missing={missing_task_ids}, unexpected={unexpected_task_ids}"
        )


def validate(
    batch_to_validate: BatchedTaskInputOutput[MinerPayload, MinerResult, MinerPublicResult],
    *,
    settings: CatValidatorSettings,
) -> BatchedTaskInputOutput[MinerPayload, ValidationResult, ValidationResult]:
    pairs = _extract_pairs(batch_to_validate)
    if len(pairs) == 0:
        return BatchedTaskInputOutput(task_input_outputs=())

    log.info("Validating batch of miner results using OpenRouter")
    scores_by_task_id = _call_openrouter_for_scores(pairs=pairs, settings=settings)
    _validate_scores_cover_all_tasks(pairs=pairs, scores_by_task_id=scores_by_task_id)

    score_lines = [
        (
            f"task_result_id={task_input_output.task_result_id} "
            f"image_name={task_input_output.task_input.input.image_name} "
            f"score={scores_by_task_id[str(task_input_output.task_result_id)]}"
        )
        for task_input_output in batch_to_validate.task_input_outputs
    ]
    log.info("Validation scores:\n%s", "\n".join(score_lines))

    return BatchedTaskInputOutput(
        task_input_outputs=tuple(
            TaskInputOutput(
                task_result_id=task_input_output.task_result_id,
                task_input=task_input_output.task_input,
                task_output=ValidationResult(score=scores_by_task_id[str(task_input_output.task_result_id)]),
                task_public_output=ValidationResult(score=scores_by_task_id[str(task_input_output.task_result_id)]),
            )
            for task_input_output in batch_to_validate.task_input_outputs
        )
    )


def weighing_func(
    mining_task_name: NexusTaskName,
    validation_task_name: NexusTaskName,
    task_results_bundle: WeightsCalculationBundle
) -> Mapping[Hotkey, Weight]:
    return {}
