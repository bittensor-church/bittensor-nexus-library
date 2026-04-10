# OpenRouter Multimodal Selection Design

## Goal

Replace the loose `selected_fields: dict[str, object]` contract in the OpenRouter payload creator with a typed multimodal selection model, while avoiding a second intermediate transport-model layer for rendered OpenRouter content blocks.

## Problem

The current branch introduces two parallel representations for multimodal data:

1. persisted selection models such as `ImageUrlSelection`
2. rendered transport models such as `ImageUrlContentBlock`

That creates unnecessary duplication for a simple pipeline:

- selectors produce `*Selection`
- the payload creator stores `*Selection`
- the payload creator immediately converts `*Selection` into `*ContentBlock`
- the communicator then treats those rendered messages as plain dict payloads anyway

The extra `*ContentBlock` layer does not add a distinct business abstraction. It only mirrors OpenRouter wire shapes under separate Python type names.

## Design Summary

Keep `*Selection` as the only typed multimodal model layer.

Each `SelectionBase` implementation should:

- store the normalized selection data
- know how to render itself into final OpenRouter message content dicts
- return final JSON-ready dict payloads directly

Delete the intermediate `*ContentBlock` typed classes entirely.

This keeps the useful typed discriminator boundary for persisted selections while removing the duplicate transport-model layer.

## Goals

- Eliminate arbitrary-object values from `selected_fields`.
- Support all OpenRouter multimodal input content types relevant to this codebase:
  - `image_url`
  - `file`
  - `input_audio`
  - `video_url`
- Remove separate `*ContentBlock` classes.
- Keep rendering logic explicit and modality-specific.
- Preserve deterministic rendering order.
- Keep scalar text rendering available for ids, labels, and metadata.

## Non-Goals

- Do not redesign `OpenRouterInferenceCommunicator`.
- Do not change task-result persistence structure beyond tightening `selected_fields`.
- Do not introduce provider-specific PDF/plugin abstractions into field selections.
- Do not support arbitrary nested JSON blobs as selection values.

## Approved Direction

The rendered OpenRouter boundary should be plain JSON-like dict payloads, not a second family of Python transport types.

That means:

- `SelectionBase` remains the typed abstraction boundary
- `OpenRouterInferenceRequest.messages` remains the outbound request representation
- rendering happens directly from `*Selection` to final dict blocks

The system should not introduce separate classes like:

- `TextContentBlock`
- `ImageUrlContentBlock`
- `FileContentBlock`
- `InputAudioContentBlock`
- `VideoUrlContentBlock`

## Proposed Types

### Scalar value

Allow simple text-renderable scalar values only:

- `str`
- `int`
- `float`
- `bool`
- `None`

These should be wrapped in `ScalarSelection` rather than stored as raw values. That keeps `selected_fields` uniform and removes the old `object` escape hatch.

```python
type ScalarValue = str | int | float | bool | None


class ScalarSelection(SelectionBase):
    kind: Literal["scalar"] = "scalar"
    value: ScalarValue
```

### Image URL

```python
class ImageUrlSelection(SelectionBase):
    kind: Literal["image_url"] = "image_url"
    url: str
```

### File

```python
class FileSelection(SelectionBase):
    kind: Literal["file"] = "file"
    filename: str
    file_data: str
```

### Input audio

```python
class InputAudioSelection(SelectionBase):
    kind: Literal["input_audio"] = "input_audio"
    data: str
    format: str
```

### Video URL

```python
class VideoUrlSelection(SelectionBase):
    kind: Literal["video_url"] = "video_url"
    url: str
```

### `SelectionValue`

`SelectionValue` should be the only allowed type inside `SelectedItem.selected_fields`.

```python
type SelectionValue = Annotated[
    ScalarSelection
    | ImageUrlSelection
    | FileSelection
    | InputAudioSelection
    | VideoUrlSelection,
    Field(discriminator="kind"),
]
```

## `SelectedItem`

Keep `SelectedItem` as the normalized stored selection container:

```python
class SelectedItem(BaseModel):
    selected_fields: dict[str, SelectionValue]
```

This remains the persisted selection boundary inside `OpenRouterInferenceRequest`.

## Rendering Contract

`SelectionBase` should own rendering to final OpenRouter content payloads.

Proposed contract:

```python
class SelectionBase(BaseModel, ABC):
    @abstractmethod
    def render_openrouter_content(
        self,
        *,
        index: int,
        field_name: str,
    ) -> list[dict[str, JsonValue]]:
        ...
```

Key point: rendering returns the final OpenRouter JSON-shaped dicts directly.

No extra `OpenRouterContentBlock` union is needed.

## Rendered Shapes

### Scalar selection

Render one text block:

```python
{"type": "text", "text": f"item[{index}].{field_name}: {value}"}
```

### Image URL selection

Render:

```python
{"type": "text", "text": f"item[{index}].{field_name}:"}
{"type": "image_url", "image_url": {"url": self.url}}
```

### File selection

Render:

```python
{"type": "text", "text": f"item[{index}].{field_name}:"}
{"type": "file", "file": {"filename": self.filename, "file_data": self.file_data}}
```

### Input audio selection

Render:

```python
{"type": "text", "text": f"item[{index}].{field_name}:"}
{"type": "input_audio", "input_audio": {"data": self.data, "format": self.format}}
```

### Video URL selection

Render:

```python
{"type": "text", "text": f"item[{index}].{field_name}:"}
{"type": "video_url", "video_url": {"url": self.url}}
```

## `OpenRouterInferenceRequest`

The persisted request should keep storing both:

- `selected_items`
- rendered `messages`

But `messages[*].content` should just be JSON-like dicts:

```python
class OpenRouterUserMessage(TypedDict):
    role: Literal["user"]
    content: list[dict[str, JsonValue]]
```

This matches the real downstream boundary more honestly. The communicator already forwards message dicts to the OpenRouter client rather than relying on separate content-block model behavior.

## Ordering

Keep the current insertion-order behavior for selected fields.

That preserves:

- original image before generated image in cat-images
- stable mixed-modality prompt ordering
- predictable persisted request payloads

No sorting should be introduced.

## Validation

The payload creator should continue validating selector output by constructing `SelectedItem`.

Failures should be explicit for:

- unsupported custom objects
- malformed multimodal selection dicts
- missing required multimodal fields
- invalid discriminator values

The system should fail at the selection-validation boundary, not silently stringify unexpected values.

## Why This Is Better

This design keeps one useful typed model boundary and removes one redundant one.

What remains valuable:

- typed discriminated selection models
- explicit modality-specific rendering behavior
- normalized persisted selection state

What is removed:

- duplicated transport type names
- shape mirroring between `*Selection` and `*ContentBlock`
- unnecessary translation code whose only job is renaming fields into wire format

## Tradeoffs

### Benefits

- less duplication
- fewer public types
- clearer ownership: selections own rendering
- easier to extend with new modalities
- transport boundary matches actual runtime behavior

### Costs

- less static precision at the final rendered-message layer
- rendered content becomes `dict[str, JsonValue]` rather than a typed Python union
- tests become the main guardrail for exact OpenRouter block shape correctness

This is an acceptable trade because the value is in typed persisted selections, while the final request payload is inherently an OpenRouter-specific JSON structure.

## Testing Strategy

Keep and extend focused tests for:

- scalar-only rendering
- image rendering
- file rendering
- input-audio rendering
- video rendering
- mixed-modality ordering
- insertion-order preservation
- rejection of unsupported selection values
- rejection of malformed multimodal selections

Cat-images tests should continue verifying that its selector returns typed selections and that the final rendered prompt content remains unchanged.

## Migration Plan

1. Keep the new `SelectionBase` and `SelectionValue` structure.
2. Delete all `*ContentBlock` and nested payload `TypedDict` classes.
3. Change `SelectionBase.render_content_blocks(...)` to `render_openrouter_content(...)` returning final dicts.
4. Update all `*Selection` implementations to render final OpenRouter dicts directly.
5. Change `OpenRouterUserMessage.content` to `list[dict[str, JsonValue]]`.
6. Update payload-creator tests to assert final message content exactly as before.
7. Keep downstream selectors such as cat-images unchanged except for any type import cleanup.

## Rationale

The system needs one typed abstraction for persisted multimodal selections. It does not need a second typed abstraction for the immediate wire-format output of those same selections.

Putting all modality data and rendering behavior on `*Selection` makes the design smaller, easier to explain, and better aligned with the actual data flow:

selector output -> normalized persisted selection -> final OpenRouter dict payload
