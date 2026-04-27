# Component Catalog

## 1) `MetagraphSource[Trigger]`

Module: `nexus/actors/metagraph_source.py`

Purpose:

- consume a typed trigger and emit a metagraph snapshot for the configured subnet

Required knobs:

- `_id`

Optional knobs:

- `netuid` (defaults from `VALIDATOR_NETUID`)
- `pylon_client_provider`

Endpoints:

- sink: `trigger`
- sources: `metagraph`, `error`

## 2) `MultiOpenRouterPayloadCreator[Item]`

Module: `nexus/actors/openrouter_payload_creator.py`

Purpose:

- convert a tuple of sampled items into an `OpenRouterInferenceRequest`
- persist both the normalized `fields` tuple and the rendered OpenRouter `messages`
- support typed multimodal prompts by turning `FieldValue` entries into explicit OpenRouter content blocks
- allow `item_selector` to skip items by returning `None`

Required knobs:

- `_id`
- `item_selector`

Optional knobs:

- `user_prompt` (defaults to `"Selected items:"`)

Selection contract:

- `item_selector` must return a `Mapping[str, FieldValue] | None`, and `Fields.fields` stores typed `*Field` values instead of arbitrary `object` values
- returning `None` skips that sampled item during projection; if every item is skipped, `MultiOpenRouterPayloadCreator` raises `ValueError`
- the reusable field models live in `nexus/actors/openrouter_selection.py`, while the task-result selector helpers live in `nexus/actors/openrouter_task_result_selection.py`
- those helpers operate on `SuccessfulTaskResult[...]` values from a task's `successful_task_result` branch
- use `select_single_task_result_metadata(...)` for universal metadata fields, `select_single_task_result_scalar_field(...)` for plain text values, and the typed helpers `select_single_task_result_image_url_field(...)`, `select_single_task_result_file_field(...)`, `select_single_task_result_input_audio_field(...)`, and `select_single_task_result_video_url_field(...)` for multimodal fields
- compose those helpers with `compose_single_task_result_selectors(...)` to build ordered task-result projections
- scalar metadata uses `ScalarField(value=...)` and renders as a `text` block like `item[0].field_name: value`
- `ImageUrlField(url=...)` renders an `image_url` block
- `FileField(filename=..., file_data=...)` renders a `file` block
- `InputAudioField(data=..., format=...)` renders an `input_audio` block
- `VideoUrlField(url=...)` renders a `video_url` block
- the supported multimodal selection kinds are `image_url`, `file`, `input_audio`, and `video_url`
- each `*Field` model renders OpenRouter content-block dicts directly; `MultiOpenRouterPayloadCreator` assembles the final `OpenRouterInferenceRequest`
- each field model persists a `kind` discriminator in its stored JSON shape
- arbitrary objects are rejected, and partial dict payloads that cannot be validated as one of the supported field models are rejected as malformed selections

Endpoints:

- sink: `input`
- sources: `created-payload`, `error`

## 3) `OpenRouterInferenceCommunicator[OutputModel]`

Module: `nexus/actors/executor_communicator/openrouter_inference_communicator.py`

Purpose:

- execute structured inference locally against OpenRouter for an `OpenRouterInferenceRequest`
- resolve an `OpenRouterClient` through the configured provider
- validate the textual model response into the configured Pydantic `output_model`

Required knobs:

- `_id`
- `output_model`

Optional knobs:

- `openrouter_client_provider`

Settings contract:

- by default, `OpenRouterInferenceCommunicator` uses `SubnetSettingsOpenRouterClientProvider`
- the default provider resolves the subnet settings object currently scoped to the runtime and builds an `OpenRouterClient` from it
- the validator settings class must implement `OpenRouterSettingsMixin`
- subnet settings lookup uses scoped subnet-settings state
- `NexusValidator.start_runtime(...)` scopes the concrete settings object for the lifetime of the runtime, and `NexusValidator.run(...)` goes through that path
- validator construction itself does not mutate global settings state
- standalone code and tests should use `with subnet_settings(settings):` before running `OpenRouterInferenceCommunicator`, unless they inject a custom `openrouter_client_provider`

Endpoints:

- sink: `input`
- sources: `processed`, `error`
