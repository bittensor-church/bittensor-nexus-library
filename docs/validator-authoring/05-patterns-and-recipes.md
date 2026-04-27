# Patterns and Recipes

## Feed metagraph snapshots from a beat

Use a beat or any other typed trigger to drive `MetagraphSource` before downstream routing or scoring steps. This lets the validator refresh its subnet view on a schedule and then pass the resulting metagraph snapshot into later graph stages.

```python
beat = EpochBeatNode("epoch-beat")
metagraph = MetagraphSource("metagraph")

Flow.from_connectable(beat.source).then(metagraph.trigger)
```

## Run structured OpenRouter inference on sampled task results

Use the reusable OpenRouter task pieces when validation should run locally against sampled successful task results and persist a structured response model. The common pattern is:

- `MultiOpenRouterPayloadCreator` to normalize the sampled tuple and render multimodal OpenRouter messages
- `NoopRouter` because the OpenRouter call happens locally, not on a subnet neuron
- `OpenRouterInferenceCommunicator` to read `OpenRouterSettingsMixin` fields from the runtime-scoped subnet settings and validate the model response
- `NoopPayloadCreator` when the validated response model should be stored as-is

Within `NexusTask`, `successful_task_result` is the persisted success branch, `executor_failure` is the persisted executor-failure branch, and `error` is the framework-failure branch. Success-only actors such as `EveryTaskResultSampler` and the shared OpenRouter task-result selector helpers should consume `successful_task_result`, not the failure branches.

```python
from datetime import timedelta
from typing import Annotated

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from nexus.actors import (
    MultiOpenRouterPayloadCreator,
    OpenRouterInferenceCommunicator,
    OpenRouterInferenceRequest,
)
from nexus.actors.neuron_router import NoopRouter
from nexus.actors.openrouter_selection import ImageUrlField, ScalarField
from nexus.actors.payload_creator import NoopPayloadCreator
from nexus.actors.retry_strategy import RetryStrategy
from nexus.actors.task_result_sampler import EveryTaskResultSampler
from nexus.core.runtime.nexus_task import NexusTask
from nexus.core.runtime.task_result_store import SuccessfulTaskResult
from nexus.utils.openrouter_config import OpenRouterSettingsMixin


class TaskScores(BaseModel):
    scores_by_task_result_id: dict[str, Annotated[int, Field(ge=1, le=100)]]


class ValidatorSettings(OpenRouterSettingsMixin, BaseSettings):
    model_config = SettingsConfigDict(env_prefix="VALIDATOR_", env_file=".env", extra="ignore")
    validation_prompt: str = "Score these items"


sampler = EveryTaskResultSampler("sampled-mining-results")

validation_task = NexusTask[
    tuple[SuccessfulTaskResult[MinerPayload, MinerResult, MinerPublicResult], ...],
    OpenRouterInferenceRequest,
    TaskScores,
](
    name=VALIDATION_TASK_NAME,
    retry=RetryStrategy("validation-task-retry", max_attempts=1, delay=timedelta(seconds=1)),
    payload_creator=MultiOpenRouterPayloadCreator[
        SuccessfulTaskResult[MinerPayload, MinerResult, MinerPublicResult]
    ](
        "validation-payload-creator",
        user_prompt=settings.validation_prompt,
        item_selector=lambda task_result: {
            "original_image_url": ImageUrlField(url=str(task_result.executor_payload.input.image_s3_url)),
            "generated_image_url": ImageUrlField(url=str(task_result.executor_public_output.presigned_url)),
            "task_result_id": ScalarField(value=str(task_result.id)),
        },
    ),
    router=NoopRouter[OpenRouterInferenceRequest]("validation-router"),
    executor_communicator=OpenRouterInferenceCommunicator[TaskScores](
        "validation-openrouter",
        output_model=TaskScores,
    ),
    executor_result_converter=NoopPayloadCreator[TaskScores]("validation-result-converter"),
)

self.connect(mining_task.successful_task_result, sampler.task_results)
self.connect(sampler.sampled_batch, validation_task.input)
```

`OpenRouterInferenceCommunicator` no longer accepts a `config_provider`. By default it resolves an `OpenRouterClient` from subnet settings state that is scoped to the active validator runtime. Validator construction itself stays pure. In normal validator startup, `NexusValidator.start_runtime(...)` temporarily registers `self.settings` for the lifetime of the runtime, and `NexusValidator.run(...)` uses that path. In standalone code or tests that invoke `OpenRouterInferenceCommunicator` directly, use `with subnet_settings(settings):` around the execution that needs OpenRouter access. If you want a different client seam, inject a custom `openrouter_client_provider`.

`MultiOpenRouterPayloadCreator` stores the normalized `fields` tuple alongside the rendered `messages`, so later stages can recover which sampled task results were scored. `Fields.fields` is a typed `dict[str, FieldValue]`, not a bag of arbitrary objects. Field values are Pydantic models:

- `ScalarField(value=...)`
- `ImageUrlField(url=...)`
- `FileField(filename=..., file_data=...)`
- `InputAudioField(data=..., format=...)`
- `VideoUrlField(url=...)`

Each `*Field` model persists a `kind` discriminator in its JSON shape and renders OpenRouter content-block dicts directly. `MultiOpenRouterPayloadCreator` then assembles the final `OpenRouterInferenceRequest`. If a selector returns an arbitrary object, or a partial dict that cannot be validated as one of those field models, `MultiOpenRouterPayloadCreator` raises a `ValueError` instead of guessing how to serialize it. In `cat-images`, the selector emits `original_image_url` and `generated_image_url` first, followed by `task_result_id`, and the validation task persists `TaskScores` as structured task output for the weighing step.

`MultiOpenRouterPayloadCreator.item_selector` may return `None` for a sampled item. Use that to skip a success-path item that should not be projected into the OpenRouter prompt, such as a result missing optional media needed by the prompt. If every sampled item is skipped, the payload creator raises `ValueError` instead of sending an empty request.

The reusable selection models live in `nexus/actors/openrouter_selection.py`. For `SuccessfulTaskResult` inputs, use the shared selector helpers in `nexus/actors/openrouter_task_result_selection.py`:

- `select_single_task_result_metadata(...)`
- `select_single_task_result_scalar_field(...)`
- `select_single_task_result_image_url_field(...)`
- `select_single_task_result_file_field(...)`
- `select_single_task_result_input_audio_field(...)`
- `select_single_task_result_video_url_field(...)`
- `compose_single_task_result_selectors(...)`

`select_single_task_result_metadata(...)` covers the universal metadata fields, while the typed extractor helpers build `ScalarField`, `ImageUrlField`, `FileField`, `InputAudioField`, and `VideoUrlField` values directly. `compose_single_task_result_selectors(...)` merges those extractors in insertion order and raises if two helpers select the same field name.

## Mix scalar metadata with direct multimodal selections

Use the shared task-result selector helpers so the selector stays explicit and type-checkable. Each helper returns `FieldValue` objects directly, and `MultiOpenRouterPayloadCreator` assembles the final `OpenRouterInferenceRequest`, so the selector stays close to the payload shape and there is no separate content-block layer to maintain.

```python
from collections.abc import Callable

from nexus.actors import FieldValue
from nexus.actors.openrouter_task_result_selection import (
    compose_single_task_result_selectors,
    select_single_task_result_file_field,
    select_single_task_result_image_url_field,
    select_single_task_result_input_audio_field,
    select_single_task_result_metadata,
    select_single_task_result_scalar_field,
    select_single_task_result_video_url_field,
)
from nexus.core.runtime.task_result_store import SuccessfulTaskResult


def build_media_review_item_selector() -> Callable[[SuccessfulTaskResult], dict[str, FieldValue]]:
    return compose_single_task_result_selectors(
        select_single_task_result_metadata(include_task_result_id=True),
        select_single_task_result_scalar_field(
            "review_instruction",
            lambda _task_result: "Compare the screenshot, transcript, narration, and reference clip.",
        ),
        select_single_task_result_image_url_field(
            "screenshot",
            lambda task_result: task_result.executor_public_output.screenshot_url,
        ),
        select_single_task_result_file_field(
            "transcript",
            filename_getter=lambda task_result: task_result.executor_payload.transcript_filename,
            file_data_getter=lambda task_result: task_result.executor_payload.transcript_file_data,
        ),
        select_single_task_result_input_audio_field(
            "narration",
            data_getter=lambda task_result: task_result.executor_payload.narration_audio_base64,
            format_getter=lambda _task_result: "wav",
        ),
        select_single_task_result_video_url_field(
            "reference_clip",
            lambda task_result: task_result.executor_public_output.demo_video_url,
        ),
    )
```

That selector mixes scalar text metadata with `ImageUrlField`, `FileField`, `InputAudioField`, and `VideoUrlField` without manually instantiating those field models. `MultiOpenRouterPayloadCreator` preserves field insertion order, so the rendered OpenRouter request keeps the same sequence shown above.
