# OpenRouter Settings Mixin Design

## Goal

Refactor OpenRouter configuration so validator processes use one canonical loaded settings object, while framework components can still resolve OpenRouter configuration implicitly.

## Problem

The current OpenRouter configuration path duplicates responsibility across:

- subnet-specific `BaseSettings` classes such as `CatValidatorSettings`
- framework-specific `OpenRouterSettings`
- runtime `OpenRouterConfig`
- adapter helpers such as `config_from_settings(...)`

That split creates two problems:

1. subnet settings duplicate framework component configuration contracts
2. framework defaults can drift from the already-loaded subnet settings object

The recent validator regression around `.env` loading showed that this split is not just cosmetic; it can break runtime behavior.

## Design Summary

Adopt a single-settings-object model:

- define an `OpenRouterSettingsMixin`
- let subnet settings classes include that mixin, for example `CatValidatorSettings(OpenRouterSettingsMixin, BaseSettings)`
- register the loaded validator settings object as current process settings
- let framework code obtain the loaded settings object through `get_current_settings_as(OpenRouterSettingsMixin)`

Remove OpenRouter-specific duplicate config types:

- remove `OpenRouterSettings`
- remove `OpenRouterConfig`
- remove OpenRouter-specific config-provider classes

The loaded validator settings object becomes the only source of truth for OpenRouter configuration inside the process.

## Goals

- Keep subnet settings explicit about what is configurable.
- Avoid redefining OpenRouter env aliases, defaults, and validation in each subnet.
- Let `OpenRouterInferenceCommunicator` configure itself implicitly in normal runtime use.
- Eliminate split-brain config between app settings and framework defaults.
- Preserve a narrow override path for tests where needed.

## Non-Goals

- Do not redesign Pylon configuration in this change.
- Do not support multiple validator settings objects in the same process.
- Do not add a general-purpose dependency injection framework.

## Proposed Components

### `OpenRouterSettingsMixin`

A reusable concrete Pydantic mixin that owns the OpenRouter configuration contract.

Implementation contract:

- `OpenRouterSettingsMixin` is a concrete base class derived from `BaseModel`
- subnet settings classes include it through multiple inheritance, for example
  `class CatValidatorSettings(OpenRouterSettingsMixin, BaseSettings): ...`
- runtime mixin checks use `isinstance(settings, OpenRouterSettingsMixin)`

This keeps the mixin usable both as:

- the owner of Pydantic fields, aliases, defaults, and validators
- the runtime type token passed to `get_current_settings_as(OpenRouterSettingsMixin)`

The mixin owns these fields:

- `openrouter_url`
- `openrouter_api_key`
- `openrouter_model`
- `validation_openrouter_timeout_seconds`
- `validation_openrouter_temperature`

It is responsible for:

- field definitions
- env aliases
- defaults, where appropriate
- validation rules

This mixin should be included directly in subnet settings classes that use OpenRouter.

### Current Settings Registry

A small process-wide settings registry and typed accessor:

- `set_current_settings(settings: BaseSettings) -> None`
- `get_current_settings_as[T](required_mixin: type[T]) -> T`
- `clear_current_settings() -> None`
- `override_current_settings(settings: BaseSettings) -> ContextManager[BaseSettings]`

Behavior:

- returns the currently registered settings object if it includes the requested mixin
- raises `ActorMisconfiguredException` if no settings are registered
- raises `ActorMisconfiguredException` if current settings do not include the requested mixin
- `set_current_settings(...)` replaces the currently registered settings object
- `override_current_settings(...)` temporarily installs a settings object and restores the previous one on exit

Ownership and lifecycle:

- in production, validator startup owns settings registration for the life of the process
- `NexusValidator.__init__(settings)` should call `set_current_settings(settings)` so both `run(...)` and direct validator construction follow the same path
- production code does not need automatic settings teardown during runtime shutdown, because the process model assumes one validator settings object for the whole process lifetime
- tests must use `override_current_settings(...)` or pair `set_current_settings(...)` with `clear_current_settings()` in teardown

The registry should be simple module-level state, not thread-local state, because the process model explicitly does not support multiple validators with different settings in one process.

This design assumes one validator process and one loaded settings object per process.

### `OpenRouterInferenceCommunicator`

Default runtime behavior:

- does not require `config_provider` to be passed explicitly
- retrieves settings with `get_current_settings_as(OpenRouterSettingsMixin)`
- reads OpenRouter fields directly from that object

Test behavior:

- default tests may register settings through the current-settings registry
- targeted override tests should use `override_current_settings(...)` rather than mutating environment variables or wiring per-communicator config providers
- no OpenRouter-specific config-provider abstraction is required after this refactor

### `openrouter_client.query(...)`

Simplify the query contract so it no longer depends on `OpenRouterConfig`.

Preferred shape:

- accept `settings: OpenRouterSettingsMixin`
- read OpenRouter fields directly from that object

This keeps runtime code aligned with the single canonical settings object model.

## Runtime Integration

`NexusValidator` already loads one `BaseSettings` instance before constructing the validator graph.

That startup flow should register the loaded settings object as current process settings before framework components that may need implicit settings resolution are used.

Direct validator construction in tests should also register settings, so the registration hook should live in validator initialization or another path shared by both `run(...)` and direct construction.

## Error Handling

Settings access should fail loudly and early:

- no current settings registered: `ActorMisconfiguredException`
- requested mixin not present on current settings: `ActorMisconfiguredException`

OpenRouter inference should not silently fall back to independent env loading once the singleton-settings model is adopted.

## Testing Strategy

Tests should cover:

- `OpenRouterSettingsMixin` validation and aliases
- current-settings registration, retrieval, and clearing
- current-settings temporary override and restoration
- failure when settings are missing
- failure when settings do not satisfy the requested mixin
- `OpenRouterInferenceCommunicator` default path using current registered settings
- subnet validator wiring no longer needing explicit OpenRouter provider adaptation

Tests that depend on current settings should clear global settings state before and after execution.

## Documentation Impact

This refactor removes public concepts that are currently documented:

- `OpenRouterConfig`
- `OpenRouterConfigProvider`
- `SettingsOpenRouterConfigProvider`
- `StaticOpenRouterConfigProvider`
- explicit `config_provider` callsite wiring in validator examples

The implementation must update validator-authoring docs so published guidance matches the singleton-settings model and `OpenRouterSettingsMixin`-based configuration path.

## Migration Plan

1. Introduce `OpenRouterSettingsMixin`.
2. Introduce current-settings registry helpers.
3. Update `CatValidatorSettings` to include the mixin.
4. Update OpenRouter runtime code to resolve settings via `get_current_settings_as(...)`.
5. Remove `OpenRouterSettings`, `OpenRouterConfig`, and OpenRouter-specific provider classes.
6. Update tests to use the singleton-settings model.
7. Update validator-authoring docs to remove the old config/provider API and describe the mixin-based singleton-settings model.

## Rationale

This design intentionally chooses one canonical settings object per validator process.

That is a better fit for this codebase than maintaining a separate framework-owned OpenRouter settings stack, because:

- validators already load one `BaseSettings` object at startup
- the process model does not need multiple validators with different settings
- the main failure mode of the current design is divergence between framework defaults and app settings

The tradeoff is explicit global runtime state. Given the process constraints, that tradeoff is acceptable and simpler than preserving duplicate config schemas.
