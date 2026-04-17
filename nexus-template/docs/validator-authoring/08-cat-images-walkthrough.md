# Cat-Images Walkthrough

## Validation flow after the OpenRouter migration

The `cat-images` validator now treats OpenRouter scoring as a normal `NexusTask` built from reusable framework actors instead of subnet-specific request wiring.

1. The mining task still handles the user request, routes it to miners, and exposes three public branches:
   - `successful_task_result` persists `SuccessfulTaskResult[MinerPayload, MinerResult, MinerPublicResult]` values.
   - `executor_failure` persists executor-side failures for retries and later accounting.
   - `error` carries framework failures that never became persisted task results.
2. `EveryTaskResultSampler` batches only `mining_task.successful_task_result` events and feeds the sampled tuple into the validation task. Executor failures never enter the OpenRouter prompt path.
3. The validator inlines an `item_selector` lambda inside `MultiOpenRouterPayloadCreator` to normalize each sampled mining result into three selected fields:
   - `task_result_id`
   - `original_image_url` from `executor_payload.input.image_s3_url`
   - `generated_image_url` from `executor_public_output.presigned_url`
4. `MultiOpenRouterPayloadCreator` renders `settings.validation_prompt` plus two `image_url` blocks per sampled result into an `OpenRouterInferenceRequest`. If a validator-specific selector chooses to skip every sampled item by returning `None`, the payload creator raises `ValueError` instead of sending an empty request.
5. `NoopRouter` keeps the validation task local while still producing the routed shape expected by the generic executor communicator contract.
6. OpenRouter settings lookup uses process-global singleton state. In normal validator startup, `NexusValidator.run(...)` initializes the concrete `CatValidatorSettings` object once before constructing the validator, and `OpenRouterInferenceCommunicator` resolves OpenRouter settings from those registered subnet settings through `OpenRouterSettingsMixin`.
7. The communicator calls the shared OpenRouter client and validates the JSON response into `TaskScores`.
8. `NoopPayloadCreator[TaskScores]` passes the validated scores through unchanged, so the validation task persists the reusable `OpenRouterInferenceRequest` as `executor_payload` and the structured `TaskScores` object as both `executor_output` and `executor_public_output`.

## Settings contract

`CatValidatorSettings` now carries the OpenRouter contract directly:

```python
class CatValidatorSettings(OpenRouterSettingsMixin, BaseSettings):
    model_config = SettingsConfigDict(env_prefix="VALIDATOR_", env_file=".env", extra="ignore")
```

That means the validator settings object itself supplies:

- `openrouter_url`
- `openrouter_api_key`
- `openrouter_model`
- `validation_openrouter_timeout_seconds`
- `validation_openrouter_temperature`

`cat-images` still overrides defaults for the OpenRouter URL, model, timeout, and temperature inside `CatValidatorSettings`, but it no longer adapts those values into separate config or provider objects.

Outside normal validator startup, code that invokes `OpenRouterInferenceCommunicator` directly should use `with subnet_settings(settings):` around the execution that needs OpenRouter access. Tests use the same scoped pattern so settings restore automatically on exit.

## Validator wiring

The validation wiring in `cat_images/validator/validator.py` now looks like this:

```python
from nexus.actors.openrouter_selection import ImageUrlField, ScalarField
from nexus.core.runtime.task_result_store import SuccessfulTaskResult


self.validation_task = NexusTask[
    tuple[SuccessfulTaskResult[MinerPayload, MinerResult, MinerPublicResult], ...],
    OpenRouterInferenceRequest,
    TaskScores,
](
    name=VALIDATION_TASK_NAME,
    retry=RetryStrategy("validation-task-retry", max_attempts=1, delay=timedelta(seconds=1.0)),
    payload_creator=MultiOpenRouterPayloadCreator[
        SuccessfulTaskResult[MinerPayload, MinerResult, MinerPublicResult]
    ](
        "create-payload-for-validation-task",
        user_prompt=settings.validation_prompt,
        item_selector=lambda task_result: {
            "original_image_url": ImageUrlField(url=str(task_result.executor_payload.input.image_s3_url)),
            "generated_image_url": ImageUrlField(url=str(task_result.executor_public_output.presigned_url)),
            "task_result_id": ScalarField(value=str(task_result.id)),
        },
    ),
    router=NoopRouter[OpenRouterInferenceRequest]("validation-router"),
    executor_communicator=OpenRouterInferenceCommunicator[TaskScores](
        "validator-communicator",
        output_model=TaskScores,
    ),
    executor_result_converter=NoopPayloadCreator[TaskScores]("validation-result-converter"),
)
```

This keeps the cat-images-specific logic small. The subnet only defines the inline field projection and the score interpretation helpers, while the framework owns prompt rendering, OpenRouter execution, task-result persistence, and empty-selection failure handling. The validation task itself follows the same three-branch contract as the mining task: `successful_task_result` for persisted scores, `executor_failure` for persisted OpenRouter executor failures, and `error` for framework failures.

## What moved out of cat-images-specific code

- `cat_images/validator/openrouter_inference.py` now defines `TaskScores`.
- `cat_images/validator/validation_algorithm.py` is reduced to `validation_result_for_score(score)`, a pure conversion from an integer score to `ValidationResult`.
- `cat_images/validator/weighing_algorithm.py` reads persisted `TaskScores` from validation task results and maps them back to mining task result ids through the stored `fields`.

## Why the persisted `TaskScores` matter

The weighting step no longer has to reconstruct scores from ad hoc prompt/output batches. It reads:

- requested mining task result ids from `validation_result.executor_payload.fields`
- returned scores from `validation_result.executor_output.scores_by_task_result_id`

That keeps the cat-images validation flow aligned with the generic OpenRouter inference pattern and makes the stored validation results reusable for later scoring logic.
