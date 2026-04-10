# OpenRouter Settings Mixin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the duplicated OpenRouter config/provider stack with an `OpenRouterSettingsMixin` plus singleton current-settings access, while keeping validator configuration explicit and preserving existing OpenRouter inference behavior.

**Architecture:** Introduce the OpenRouter configuration contract and the singleton current-settings registry together while temporarily keeping the old OpenRouter config/provider API in place so existing consumers continue to work. Then migrate the OpenRouter client and communicator to use the registered settings object implicitly, update validator startup and cat-images wiring to register and consume those settings, and only then remove the old OpenRouter config/provider API and update docs/tests.

**Tech Stack:** Python 3.14, Pydantic, pydantic-settings, pytest, Nexus runtime

---

## File Map

- Create: `nexus-template/nexus/utils/current_settings.py`
  Responsibility: own the singleton current-settings registry, typed access helper, and temporary override context manager.
- Modify: `nexus-template/nexus/utils/openrouter_config.py`
  Responsibility: introduce `OpenRouterSettingsMixin` first, then remove `OpenRouterSettings`, `OpenRouterConfig`, and provider classes after consumers migrate.
- Modify: `nexus-template/nexus/utils/openrouter_client.py`
  Responsibility: consume `OpenRouterSettingsMixin` directly instead of `OpenRouterConfig` or ad hoc settings adapters.
- Modify: `nexus-template/nexus/actors/executor_communicator/openrouter_inference_communicator.py`
  Responsibility: resolve OpenRouter settings through `get_current_settings_as(OpenRouterSettingsMixin)` and remove explicit provider wiring.
- Modify: `nexus-template/nexus/nexus_validator.py`
  Responsibility: register the loaded validator settings object as current process settings during validator construction.
- Modify: `nexus-template/tests/conftest.py`
  Responsibility: provide cleanup for current-settings global state between framework tests.
- Modify: `nexus-template/tests/test_openrouter_config.py`
  Responsibility: cover mixin validation, registry behavior, override restoration, and wrong-mixin failures.
- Modify: `nexus-template/tests/test_openrouter_inference_communicator.py`
  Responsibility: verify communicator default behavior with registered settings and missing-settings failures.
- Modify: `nexus-template/tests/test_openrouter_inference_task.py`
  Responsibility: update end-to-end task wiring to use registered settings instead of explicit static providers.
- Modify: `cat-images/cat_images/validator/validator_settings.py`
  Responsibility: include `OpenRouterSettingsMixin` and remove duplicated OpenRouter field declarations.
- Modify: `cat-images/cat_images/validator/validator.py`
  Responsibility: stop passing explicit OpenRouter config/provider adaptation and rely on implicit singleton settings resolution.
- Create: `cat-images/tests/conftest.py`
  Responsibility: clear current-settings global state between cat-images tests.
- Modify: `cat-images/tests/test_validator_openrouter_config.py`
  Responsibility: prove validator wiring uses the passed settings object through singleton registration.
- Modify: `nexus-template/docs/validator-authoring/reference/component-catalog.md`
  Responsibility: remove the old OpenRouter config/provider API and document the mixin-based singleton-settings model.
- Modify: `nexus-template/docs/validator-authoring/05-patterns-and-recipes.md`
  Responsibility: update the OpenRouter recipe to rely on `OpenRouterSettingsMixin` plus implicit current settings.
- Modify: `nexus-template/docs/validator-authoring/08-cat-images-walkthrough.md`
  Responsibility: update the walkthrough to reflect implicit settings resolution and the removal of explicit provider wiring.

### Task 1: Introduce `OpenRouterSettingsMixin` and current-settings registry together

**Files:**
- Create: `nexus-template/nexus/utils/current_settings.py`
- Modify: `nexus-template/nexus/utils/openrouter_config.py`
- Modify: `nexus-template/tests/conftest.py`
- Modify: `nexus-template/tests/test_openrouter_config.py`
- Modify: `cat-images/cat_images/validator/validator_settings.py`
- Create: `cat-images/tests/conftest.py`

- [ ] **Step 1: Add failing mixin and registry tests**

```python
class _TestValidatorSettings(OpenRouterSettingsMixin, BaseSettings):
    model_config = SettingsConfigDict(env_prefix="VALIDATOR_", extra="ignore")


def test_openrouter_settings_mixin_reads_validator_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VALIDATOR_OPENROUTER_URL", "https://router.test/api")
    ...
    settings = _TestValidatorSettings()
    assert settings.openrouter_url == "https://router.test/api"


def test_get_current_settings_as_returns_registered_settings() -> None:
    settings = _TestValidatorSettings(...)
    set_current_settings(settings)
    assert get_current_settings_as(OpenRouterSettingsMixin) is settings


def test_get_current_settings_as_rejects_missing_settings() -> None:
    clear_current_settings()
    with pytest.raises(ActorMisconfiguredException):
        get_current_settings_as(OpenRouterSettingsMixin)


def test_override_current_settings_restores_previous_settings() -> None:
    outer = _TestValidatorSettings(...)
    inner = _TestValidatorSettings(...)
    set_current_settings(outer)
    with override_current_settings(inner):
        assert get_current_settings_as(OpenRouterSettingsMixin) is inner
    assert get_current_settings_as(OpenRouterSettingsMixin) is outer


def test_get_current_settings_as_rejects_wrong_mixin() -> None:
    set_current_settings(_NonOpenRouterSettings(...))
    with pytest.raises(ActorMisconfiguredException):
        get_current_settings_as(OpenRouterSettingsMixin)
```

- [ ] **Step 2: Add test cleanup ownership for global settings**

Add autouse fixtures in both test packages:

```python
@pytest.fixture(autouse=True)
def _clear_current_settings_between_tests() -> Iterator[None]:
    clear_current_settings()
    yield
    clear_current_settings()
```

- [ ] **Step 3: Run the focused config test file to verify the new assertions fail**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && PYTHONPATH=. uv run pytest -q --tb=line -r f tests/test_openrouter_config.py`
Expected: FAIL because `OpenRouterSettingsMixin` and current-settings helpers do not exist yet.

- [ ] **Step 4: Implement `OpenRouterSettingsMixin` and the current-settings registry**

```python
# nexus-template/nexus/utils/openrouter_config.py
class OpenRouterSettingsMixin(BaseModel):
    openrouter_url: str = Field(validation_alias=AliasChoices("OPENROUTER_URL", "VALIDATOR_OPENROUTER_URL"))
    openrouter_api_key: str = Field(validation_alias=AliasChoices("OPENROUTER_API_KEY", "VALIDATOR_OPENROUTER_API_KEY"))
    openrouter_model: str = Field(validation_alias=AliasChoices("OPENROUTER_MODEL", "VALIDATOR_OPENROUTER_MODEL"))
    validation_openrouter_timeout_seconds: float = Field(...)
    validation_openrouter_temperature: float = Field(...)
```

```python
# nexus-template/nexus/utils/current_settings.py
_CURRENT_SETTINGS: BaseSettings | None = None

def set_current_settings(settings: BaseSettings) -> None: ...
def clear_current_settings() -> None: ...
def get_current_settings_as[T](required_mixin: type[T]) -> T: ...
@contextmanager
def override_current_settings(settings: BaseSettings) -> Generator[BaseSettings]: ...
```

Do **not** remove the old OpenRouter config/provider API in this task. Keep:
- `OpenRouterConfig`
- `OpenRouterSettings`
- provider classes
- `config_from_settings(...)`
- `clear_openrouter_settings_cache()`

temporarily in place so existing OpenRouter consumers and tests still pass while the new mixin/registry path is introduced.

- [ ] **Step 5: Make `CatValidatorSettings` inherit the mixin and delete duplicated OpenRouter fields**

```python
class CatValidatorSettings(OpenRouterSettingsMixin, BaseSettings):
    ...
```

- [ ] **Step 6: Re-run the focused config tests**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && PYTHONPATH=. uv run pytest -q --tb=line -r f tests/test_openrouter_config.py`
Expected: PASS

- [ ] **Step 7: Run package QA in `nexus-template`**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && uv run ruff check --fix && uv run ruff format`
Expected: PASS

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && uv run basedpyright`
Expected: PASS or only pre-existing unrelated failures already known on the branch. If there are new OpenRouter-related failures, fix them before committing.

- [ ] **Step 8: Run focused `cat-images` verification for the settings-class change**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/cat-images && PYTHONPATH=. uv run pytest -q --tb=line -r f tests/test_validator_openrouter_config.py`
Expected: PASS or FAIL only because later singleton-registration work is not implemented yet; there should be no new import or settings-schema breakage from the mixin change itself.

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/cat-images && uv run ruff check --fix && uv run ruff format`
Expected: PASS

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/cat-images && uv run basedpyright`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git -C /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task add \
  nexus-template/nexus/utils/current_settings.py \
  nexus-template/nexus/utils/openrouter_config.py \
  nexus-template/tests/conftest.py \
  nexus-template/tests/test_openrouter_config.py \
  cat-images/cat_images/validator/validator_settings.py \
  cat-images/tests/conftest.py
git -C /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task commit -m "refactor(openrouter): add settings mixin and registry"
```

### Task 2: Migrate OpenRouter runtime code to implicit current-settings resolution

**Files:**
- Modify: `nexus-template/nexus/utils/openrouter_config.py`
- Modify: `nexus-template/nexus/utils/openrouter_client.py`
- Modify: `nexus-template/nexus/actors/executor_communicator/openrouter_inference_communicator.py`
- Modify: `nexus-template/tests/test_openrouter_config.py`
- Modify: `nexus-template/tests/test_openrouter_inference_communicator.py`
- Modify: `nexus-template/tests/test_openrouter_inference_task.py`

- [ ] **Step 1: Add failing communicator and task tests for implicit settings resolution**

```python
def test_openrouter_inference_communicator_uses_current_registered_settings(...) -> None:
    with override_current_settings(_TestValidatorSettings(...)):
        ...
        assert captured["url"] == "https://router.test/api"


def test_openrouter_inference_communicator_emits_error_when_required_settings_missing(...) -> None:
    clear_current_settings()
    ...
    assert len(setup.error_collector.received_events) == 1
```

Update the task-level test to wrap the composed task execution in `override_current_settings(...)` instead of passing an explicit static provider.

- [ ] **Step 2: Run communicator and task tests to verify they fail**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && PYTHONPATH=. uv run pytest -q --tb=line -r f tests/test_openrouter_inference_communicator.py tests/test_openrouter_inference_task.py`
Expected: FAIL because old provider/config APIs are still referenced.

- [ ] **Step 3: Simplify `openrouter_client.query(...)`**

```python
def query[ResponseModelT: BaseModel](
    *,
    messages: list[dict[str, Any]],
    settings: OpenRouterSettingsMixin,
    response_model: type[ResponseModelT],
) -> ResponseModelT:
    payload = {
        "model": settings.openrouter_model,
        "temperature": settings.validation_openrouter_temperature,
        "messages": messages,
    }
```

- [ ] **Step 4: Remove provider wiring from `OpenRouterInferenceCommunicator`**

```python
settings = get_current_settings_as(OpenRouterSettingsMixin)
output = openrouter_client.query(
    messages=messages,
    settings=settings,
    response_model=self.communicator_spec.output_model,
)
```

Remove:
- `config_provider` field and constructor argument
- imports of deleted config/provider symbols

- [ ] **Step 5: Re-run the focused OpenRouter tests**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && PYTHONPATH=. uv run pytest -q --tb=line -r f tests/test_openrouter_config.py tests/test_openrouter_inference_communicator.py tests/test_openrouter_inference_task.py`
Expected: PASS

- [ ] **Step 6: Run package QA in `nexus-template`**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && uv run ruff check --fix && uv run ruff format`
Expected: PASS

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && uv run basedpyright`
Expected: PASS or only pre-existing unrelated failures already known on the branch.

- [ ] **Step 7: Commit**

```bash
git -C /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task add \
  nexus-template/nexus/utils/openrouter_config.py \
  nexus-template/nexus/utils/openrouter_client.py \
  nexus-template/nexus/actors/executor_communicator/openrouter_inference_communicator.py \
  nexus-template/tests/test_openrouter_config.py \
  nexus-template/tests/test_openrouter_inference_communicator.py \
  nexus-template/tests/test_openrouter_inference_task.py
git -C /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task commit -m "refactor(openrouter): resolve current settings implicitly"
```

### Task 3: Register settings in validator startup and remove the old OpenRouter config/provider API

**Files:**
- Modify: `nexus-template/nexus/utils/openrouter_config.py`
- Modify: `nexus-template/nexus/nexus_validator.py`
- Modify: `cat-images/cat_images/validator/validator.py`
- Modify: `cat-images/tests/test_validator_openrouter_config.py`

- [ ] **Step 1: Add a failing validator-registration regression test**

```python
def test_validator_validation_task_uses_registered_settings_for_openrouter_config(...) -> None:
    validator = Validator(_build_settings())
    communicator = validator.validation_task.executor_communicator
    settings = get_current_settings_as(OpenRouterSettingsMixin)
    assert settings.openrouter_url == "https://settings.test/api"
```

This test should clear current settings before and after via the package autouse fixture.

- [ ] **Step 2: Run the focused cat-images regression test to verify it fails**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/cat-images && PYTHONPATH=. uv run pytest -q --tb=line -r f tests/test_validator_openrouter_config.py`
Expected: FAIL until validator registration is implemented.

- [ ] **Step 3: Register settings in `NexusValidator.__init__(settings)`**

```python
class NexusValidator:
    def __init__(self, settings: BaseSettings) -> None:
        set_current_settings(settings)
        ...
```

- [ ] **Step 4: Remove explicit OpenRouter config adaptation from cat-images validator wiring**

Delete:
- `StaticOpenRouterConfigProvider`
- `config_from_settings(settings)`
- explicit `config_provider=...`

- [ ] **Step 5: Remove the old OpenRouter config/provider API from `openrouter_config.py`**

Delete:
- `OpenRouterConfig`
- `OpenRouterSettings`
- `OpenRouterConfigProvider`
- `SettingsOpenRouterConfigProvider`
- `StaticOpenRouterConfigProvider`
- `config_from_settings(...)`
- `clear_openrouter_settings_cache()`

- [ ] **Step 6: Re-run the focused cat-images regression test**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/cat-images && PYTHONPATH=. uv run pytest -q --tb=line -r f tests/test_validator_openrouter_config.py`
Expected: PASS

- [ ] **Step 7: Run focused `nexus-template` verification for the API removal and validator registration hook**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && PYTHONPATH=. uv run pytest -q --tb=line -r f tests/test_openrouter_config.py tests/test_openrouter_inference_communicator.py tests/test_openrouter_inference_task.py tests/test_nexus_validator.py`
Expected: PASS

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && uv run ruff check --fix && uv run ruff format`
Expected: PASS

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && uv run basedpyright`
Expected: PASS or only pre-existing unrelated failures already known on the branch.

- [ ] **Step 8: Run package QA in `cat-images`**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/cat-images && uv run ruff check --fix && uv run ruff format`
Expected: PASS

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/cat-images && uv run basedpyright`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git -C /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task add \
  nexus-template/nexus/utils/openrouter_config.py \
  nexus-template/nexus/nexus_validator.py \
  cat-images/cat_images/validator/validator.py \
  cat-images/tests/test_validator_openrouter_config.py
git -C /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task commit -m "refactor(nexus): register current validator settings"
```

### Task 4: Update docs and run final targeted verification

**Files:**
- Modify: `nexus-template/docs/validator-authoring/reference/component-catalog.md`
- Modify: `nexus-template/docs/validator-authoring/05-patterns-and-recipes.md`
- Modify: `nexus-template/docs/validator-authoring/08-cat-images-walkthrough.md`

- [ ] **Step 1: Update docs to remove the old OpenRouter config/provider API**

Required doc changes:
- remove mentions of `OpenRouterConfig`, `OpenRouterConfigProvider`, `SettingsOpenRouterConfigProvider`, and `StaticOpenRouterConfigProvider`
- document `OpenRouterSettingsMixin`
- document implicit settings resolution through the current registered validator settings
- update code examples so `OpenRouterInferenceCommunicator` no longer takes `config_provider`

- [ ] **Step 2: Run the doc grep check**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && rg -n "OpenRouterConfig|OpenRouterConfigProvider|OpenRouterSettingsMixin|OpenRouterInferenceCommunicator|cat-images" docs/validator-authoring`
Expected: only the new mixin/singleton-settings model appears in the relevant docs.

- [ ] **Step 3: Run final targeted framework tests**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/nexus-template && PYTHONPATH=. uv run pytest -q --tb=line -r f tests/test_openrouter_config.py tests/test_openrouter_payload_creator.py tests/test_openrouter_inference_communicator.py tests/test_openrouter_inference_task.py`
Expected: PASS

- [ ] **Step 4: Run final targeted cat-images tests**

Run: `cd /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task/cat-images && PYTHONPATH=. uv run pytest -q --tb=line -r f tests/test_validation_inference.py tests/test_validator_openrouter_config.py tests/test_weighing_algorithm.py`
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

- [ ] **Step 6: Commit**

```bash
git -C /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task add \
  nexus-template/docs/validator-authoring/reference/component-catalog.md \
  nexus-template/docs/validator-authoring/05-patterns-and-recipes.md \
  nexus-template/docs/validator-authoring/08-cat-images-walkthrough.md
git -C /home/kuba/repos/nexus-poc/.worktrees/openrouter-inference-task commit -m "docs(nexus-template): update OpenRouter settings guidance"
```
