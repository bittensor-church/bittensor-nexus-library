# OpenRouter Multimodal Selection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the intermediate `*ContentBlock` layer and make each `*Selection` render final OpenRouter content dicts directly, while preserving existing request payload behavior.

**Architecture:** Keep `SelectionBase` and the discriminated `SelectionValue` union as the only typed multimodal model layer. Move final OpenRouter wire-shape rendering into the `*Selection` classes themselves, then update the payload creator to consume that simpler rendering contract and keep the rest of the request pipeline unchanged.

**Tech Stack:** Python 3.14, Pydantic, `JsonValue`, pytest, basedpyright, ruff

---

## File Map

- Modify: `nexus-template/nexus/actors/openrouter_selection.py`
  Responsibility: remove the `*ContentBlock` transport types, define one JSON-like rendered content alias if needed, and make each `*Selection` render final OpenRouter dict payloads directly.
- Modify: `nexus-template/nexus/actors/openrouter_payload_creator.py`
  Responsibility: switch the payload creator from `OpenRouterContentBlock` to the new direct-rendering contract and keep persisted `messages` typing aligned with the actual OpenRouter request shape.
- Create: `nexus-template/tests/test_openrouter_selection.py`
  Responsibility: pin the new selection-owned rendering API directly on `ScalarSelection`, `ImageUrlSelection`, `FileSelection`, `InputAudioSelection`, and `VideoUrlSelection`.
- Modify: `nexus-template/tests/test_openrouter_payload_creator.py`
  Responsibility: keep end-to-end payload-creator regression coverage and add assertions that remain meaningful after the transport-layer types are removed.
- Modify: `nexus-template/docs/validator-authoring/reference/component-catalog.md`
  Responsibility: document that `MultiOpenRouterPayloadCreator` stores typed selections and renders final OpenRouter request dicts directly from them.
- Modify: `nexus-template/docs/validator-authoring/05-patterns-and-recipes.md`
  Responsibility: show a multimodal selector recipe using `*Selection` classes without mentioning `*ContentBlock` transport models.

## Task 1: Pin direct selection rendering with failing tests

**Files:**
- Create: `nexus-template/tests/test_openrouter_selection.py`

- [ ] **Step 1: Write failing tests for scalar and multimodal selection rendering**

Create a focused test file that exercises the new `SelectionBase` contract directly:

```python
from nexus.actors.openrouter_selection import (
    FileSelection,
    ImageUrlSelection,
    InputAudioSelection,
    ScalarSelection,
    VideoUrlSelection,
)


def test_scalar_selection_renders_final_text_dict() -> None:
    assert ScalarSelection(value="a").render_openrouter_content(
        index=0,
        field_name="task_result_id",
    ) == [
        {"type": "text", "text": "item[0].task_result_id: a"},
    ]


def test_image_url_selection_renders_label_then_image_dict() -> None:
    assert ImageUrlSelection(url="https://example.com/a.png").render_openrouter_content(
        index=0,
        field_name="image",
    ) == [
        {"type": "text", "text": "item[0].image:"},
        {"type": "image_url", "image_url": {"url": "https://example.com/a.png"}},
    ]


def test_file_selection_renders_label_then_file_dict() -> None:
    assert FileSelection(
        filename="notes.txt",
        file_data="data:text/plain;base64,SGVsbG8=",
    ).render_openrouter_content(
        index=0,
        field_name="attachment",
    ) == [
        {"type": "text", "text": "item[0].attachment:"},
        {
            "type": "file",
            "file": {
                "filename": "notes.txt",
                "file_data": "data:text/plain;base64,SGVsbG8=",
            },
        },
    ]
```

Add matching tests for `InputAudioSelection` and `VideoUrlSelection`.

- [ ] **Step 2: Run the new selection test file to verify it fails**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && PYTHONPATH=. uv run pytest -q --tb=line -r f tests/test_openrouter_selection.py`

Expected: FAIL with `AttributeError` or type errors because `render_openrouter_content(...)` does not exist yet and the old `render_content_blocks(...)` API is still in place.

- [ ] **Step 3: Commit the red tests**

```bash
git -C /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task add \
  nexus-template/tests/test_openrouter_selection.py
git -C /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task commit -m "test(openrouter): pin direct selection rendering"
```

## Task 2: Remove the `*ContentBlock` layer from `openrouter_selection.py`

**Files:**
- Modify: `nexus-template/nexus/actors/openrouter_selection.py`
- Modify: `nexus-template/tests/test_openrouter_selection.py`

- [ ] **Step 1: Replace the `*ContentBlock` transport types with one rendered-content alias**

In `nexus-template/nexus/actors/openrouter_selection.py`, remove:

- `TextContentBlock`
- `ImageUrlPayload`
- `ImageUrlContentBlock`
- `FilePayload`
- `FileContentBlock`
- `InputAudioPayload`
- `InputAudioContentBlock`
- `VideoUrlPayload`
- `VideoUrlContentBlock`
- `OpenRouterContentBlock`

Replace them with one alias for the final rendered shape:

```python
from pydantic import BaseModel, ConfigDict, Field, JsonValue

type OpenRouterMessageContent = dict[str, JsonValue]
```

- [ ] **Step 2: Rename the abstract rendering API to reflect the direct boundary**

Change:

```python
def render_content_blocks(self, *, index: int, field_name: str) -> list[OpenRouterContentBlock]:
```

to:

```python
def render_openrouter_content(
    self,
    *,
    index: int,
    field_name: str,
) -> list[OpenRouterMessageContent]:
```

- [ ] **Step 3: Update helper functions to return final dict payloads**

Keep `text_content_block()` and `_label_block()` if they still help readability, but make them return `OpenRouterMessageContent` directly:

```python
def text_content_block(text: str) -> OpenRouterMessageContent:
    return {"type": "text", "text": text}
```

- [ ] **Step 4: Update each `*Selection` class to render final OpenRouter dicts**

Implement:

```python
class ScalarSelection(SelectionBase):
    ...
    def render_openrouter_content(...) -> list[OpenRouterMessageContent]:
        return [{"type": "text", "text": f"item[{index}].{field_name}: {self.rendered_value()}"}]
```

```python
class ImageUrlSelection(SelectionBase):
    ...
    def render_openrouter_content(...) -> list[OpenRouterMessageContent]:
        return [
            {"type": "text", "text": f"item[{index}].{field_name}:"},
            {"type": "image_url", "image_url": {"url": self.url}},
        ]
```

Do the same for `FileSelection`, `InputAudioSelection`, and `VideoUrlSelection`.

- [ ] **Step 5: Remove stale exports tied to the deleted transport layer**

Delete `OpenRouterContentBlock` from `__all__` in `nexus-template/nexus/actors/openrouter_selection.py`.

Keep these public exports:

- `ScalarSelection`
- `ImageUrlSelection`
- `FileSelection`
- `InputAudioSelection`
- `VideoUrlSelection`
- `SelectionValue`
- `SelectedItem`

- [ ] **Step 6: Run the direct selection tests to verify they pass**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && PYTHONPATH=. uv run pytest -q --tb=line -r f tests/test_openrouter_selection.py`

Expected: PASS

- [ ] **Step 7: Commit the selection-layer refactor**

```bash
git -C /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task add \
  nexus-template/nexus/actors/openrouter_selection.py \
  nexus-template/tests/test_openrouter_selection.py
git -C /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task commit -m "refactor(openrouter): render final content from selections"
```

## Task 3: Rewire the payload creator to the direct-rendering contract

**Files:**
- Modify: `nexus-template/nexus/actors/openrouter_payload_creator.py`
- Modify: `nexus-template/tests/test_openrouter_payload_creator.py`

- [ ] **Step 1: Add one focused regression test that exercises the new method through the payload creator**

In `nexus-template/tests/test_openrouter_payload_creator.py`, add one explicit regression test that keeps the existing visible behavior pinned while the internals change:

```python
def test_multi_openrouter_payload_creator_uses_selection_rendering_contract(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    creator = MultiOpenRouterPayloadCreator[PromptItem](
        "openrouter-payload-creator",
        item_selector=lambda item: {
            "title": ScalarSelection(value=item.title),
            "image": ImageUrlSelection(url=item.image_url),
        },
    )
    setup = transform_actor_test_setup_factory(creator)

    with setup.running():
        setup.send(input_payload=(_sample_prompt_item(),))
        wait_until(lambda: len(setup.processed_collector.received_events) == 1)

    request = setup.processed_collector.received_events[0].payload
    assert request.messages[0]["content"] == [
        {"type": "text", "text": "Selected items:"},
        {"type": "text", "text": "item[0].title: Alpha"},
        {"type": "text", "text": "item[0].image:"},
        {"type": "image_url", "image_url": {"url": "https://example.com/a.png"}},
    ]
```

The existing tests already cover most output shapes, so keep them and add only the minimal new regression if it strengthens the internal-contract change.

- [ ] **Step 2: Run the payload-creator tests to verify they fail against the old API**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && PYTHONPATH=. uv run pytest -q --tb=line -r f tests/test_openrouter_payload_creator.py`

Expected: FAIL because `openrouter_payload_creator.py` still imports `OpenRouterContentBlock` and still calls `render_content_blocks(...)`.

- [ ] **Step 3: Update `openrouter_payload_creator.py` to use the new rendered-content contract**

Change imports so `nexus-template/nexus/actors/openrouter_payload_creator.py` no longer depends on `OpenRouterContentBlock`.

Use either:

```python
from nexus.actors.openrouter_selection import OpenRouterMessageContent
```

or direct `dict[str, JsonValue]` annotations if that is clearer.

Then update:

```python
class OpenRouterUserMessage(TypedDict):
    role: Literal["user"]
    content: list[OpenRouterMessageContent]
```

and:

```python
def _render_content(self, selected_items: tuple[SelectedItem, ...]) -> list[OpenRouterMessageContent]:
    content: list[OpenRouterMessageContent] = [text_content_block(self.creator_spec.user_prompt)]
    for index, selected_item in enumerate(selected_items):
        for field_name, value in selected_item.selected_fields.items():
            content.extend(value.render_openrouter_content(index=index, field_name=field_name))
    return content
```

- [ ] **Step 4: Re-run the payload-creator tests**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && PYTHONPATH=. uv run pytest -q --tb=line -r f tests/test_openrouter_payload_creator.py`

Expected: PASS

- [ ] **Step 5: Run inference-task regression coverage**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && PYTHONPATH=. uv run pytest -q --tb=line -r f tests/test_openrouter_inference_task.py`

Expected: PASS

- [ ] **Step 6: Commit the payload-creator update**

```bash
git -C /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task add \
  nexus-template/nexus/actors/openrouter_payload_creator.py \
  nexus-template/tests/test_openrouter_payload_creator.py
git -C /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task commit -m "refactor(openrouter): drop content block transport types"
```

## Task 4: Update docs and run final verification

**Files:**
- Modify: `nexus-template/docs/validator-authoring/reference/component-catalog.md`
- Modify: `nexus-template/docs/validator-authoring/05-patterns-and-recipes.md`

- [ ] **Step 1: Update the validator-authoring docs to describe the simplified boundary**

Document these points explicitly:

- `SelectedItem.selected_fields` stores typed `*Selection` values
- each `*Selection` renders final OpenRouter request dicts directly
- there is no separate `*ContentBlock` model layer
- supported multimodal selection kinds are:
  - `image_url`
  - `file`
  - `input_audio`
  - `video_url`

- [ ] **Step 2: Add one multimodal recipe using the `*Selection` classes directly**

Include a concrete example such as:

```python
creator = MultiOpenRouterPayloadCreator[PromptItem](
    "multimodal-example",
    item_selector=lambda item: {
        "task_result_id": ScalarSelection(value=item.item_id),
        "image": ImageUrlSelection(url=item.image_url),
        "attachment": FileSelection(
            filename="notes.txt",
            file_data="data:text/plain;base64,SGVsbG8=",
        ),
    },
)
```

Describe that the payload creator will persist those selections and render final OpenRouter `messages[*].content` dicts from them.

- [ ] **Step 3: Run the doc grep check**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && rg -n "ContentBlock|render_openrouter_content|ImageUrlSelection|FileSelection|InputAudioSelection|VideoUrlSelection" docs/validator-authoring`

Expected:

- updated docs mention `render_openrouter_content` and the `*Selection` types
- no validator-authoring doc still presents `*ContentBlock` as the public abstraction

- [ ] **Step 4: Run final targeted regression tests**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && PYTHONPATH=. uv run pytest -q --tb=line -r f tests/test_openrouter_selection.py tests/test_openrouter_payload_creator.py tests/test_openrouter_inference_task.py`

Expected: PASS

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/cat-images && PYTHONPATH=. uv run pytest -q --tb=line -r f tests/test_validation_inference.py tests/test_weighing_algorithm.py`

Expected: PASS

- [ ] **Step 5: Run final package QA**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && uv run ruff check --fix && uv run ruff format`

Expected: PASS

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && uv run basedpyright`

Expected: PASS or only pre-existing unrelated failures already known on the branch.

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/cat-images && uv run ruff check --fix && uv run ruff format`

Expected: PASS

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/cat-images && uv run basedpyright`

Expected: PASS

- [ ] **Step 6: Commit the docs update**

```bash
git -C /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task add \
  nexus-template/docs/validator-authoring/reference/component-catalog.md \
  nexus-template/docs/validator-authoring/05-patterns-and-recipes.md
git -C /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task commit -m "docs(openrouter): document direct selection rendering"
```
