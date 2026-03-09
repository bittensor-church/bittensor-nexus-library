import logging
from typing import Annotated, Any

from nexus.actors.task_input_output_creator import BatchedTaskInputOutput, TaskInputOutput
from nexus.utils import openrouter_client
from pydantic import BaseModel, Field

from cat_images.subnet_models import MinerPayload, MinerPublicResult, MinerResult, ValidationResult

from .validator_settings import CatValidatorSettings

log = logging.getLogger("validation-algorithm")


class TaskScores(BaseModel):
    scores_by_task_result_id: dict[str, Annotated[int, Field(ge=1, le=100)]] = Field(default_factory=dict)


class ImagePair(BaseModel):
    task_result_id: str
    original_image_url: str
    generated_image_url: str


def extract_image_pairs(
    batch_to_validate: BatchedTaskInputOutput[MinerPayload, MinerResult, MinerPublicResult],
) -> tuple[ImagePair, ...]:
    return tuple(
        ImagePair(
            task_result_id=str(task_input_output.task_result_id),
            original_image_url=str(task_input_output.task_input.input.image_s3_url),
            generated_image_url=str(task_input_output.task_public_output.presigned_url),
        )
        for task_input_output in batch_to_validate.task_input_outputs
    )



def _build_validation_messages(
    *,
    pairs: tuple[ImagePair, ...],
    settings: CatValidatorSettings,
) -> list[dict[str, Any]]:
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

    return [
        {
            "role": "user",
            "content": content,
        }
    ]


def validate(
    batch_to_validate: BatchedTaskInputOutput[MinerPayload, MinerResult, MinerPublicResult],
    *,
    settings: CatValidatorSettings,
) -> BatchedTaskInputOutput[MinerPayload, ValidationResult, ValidationResult]:
    pairs = extract_image_pairs(batch_to_validate)
    if len(pairs) == 0:
        return BatchedTaskInputOutput(task_input_outputs=())

    log.info("Validating batch of miner results using OpenRouter")
    scored = openrouter_client.query(
        messages=_build_validation_messages(pairs=pairs, settings=settings),
        settings=settings,
        response_model=TaskScores,
    )
    scores_by_task_id = scored.scores_by_task_result_id

    score_lines = [
        (
            f"task_result_id={task_input_output.task_result_id} "
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
