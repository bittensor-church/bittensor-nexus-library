# OpenRouter Client Injection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the module-level OpenRouter `query()` helper with a concrete `OpenRouterClient` and inject it into the OpenRouter communicator through a provider.

**Architecture:** Keep the HTTP request and response parsing logic in `nexus/utils/openrouter_client.py`, but move it onto an `OpenRouterClient` instance that binds settings at construction time. Add `nexus/actors/openrouter_client_provider.py` to build the default client from subnet settings, and change `OpenRouterInferenceCommunicator` to depend on that provider so tests can inject a mocked `OpenRouterClient`.

**Tech Stack:** Python 3.14, pytest, httpx, pydantic, basedpyright

---

### Task 1: Red Tests For Client Injection

**Files:**
- Modify: `tests/test_openrouter_inference_communicator.py`
- Modify: `tests/test_openrouter_config.py`
- Create: `tests/test_openrouter_client_provider.py`

- [ ] **Step 1: Write the failing tests**

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest -q --tb=line -r f tests/test_openrouter_inference_communicator.py tests/test_openrouter_config.py tests/test_openrouter_client_provider.py`
Expected: FAIL because `OpenRouterInferenceCommunicator` does not yet accept an injected provider and `openrouter_client.py` does not yet expose the class-based API.

### Task 2: Implement Client And Provider

**Files:**
- Modify: `nexus/utils/openrouter_client.py`
- Create: `nexus/actors/openrouter_client_provider.py`
- Modify: `nexus/actors/executor_communicator/openrouter_inference_communicator.py`
- Modify: `nexus/actors/__init__.py`

- [ ] **Step 1: Implement the minimal production code**

- [ ] **Step 2: Run the targeted tests**

Run: `uv run pytest -q --tb=line -r f tests/test_openrouter_inference_communicator.py tests/test_openrouter_config.py tests/test_openrouter_client_provider.py`
Expected: PASS

### Task 3: Update Docs And Verify

**Files:**
- Modify: `docs/validator-authoring/reference/component-catalog.md`

- [ ] **Step 1: Update public docs for the new optional communicator knob**

- [ ] **Step 2: Run project checks for touched areas**

Run: `uv run ruff check --fix && uv run ruff format && uv run basedpyright && uv run pytest -q --tb=line -r f tests/test_openrouter_inference_communicator.py tests/test_openrouter_config.py tests/test_openrouter_client_provider.py`
Expected: PASS
