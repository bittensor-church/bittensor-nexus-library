# Nexus

Opinionated event-driven actor framework for building Bittensor subnets. Thread-based concurrency, message-based flows.

This library now lives in the `bittensor-nexus-library` subdirectory. It keeps the existing `nexus` package and public
API used by `cat-images`, while also adopting the template-derived project scaffolding that belongs with the new
directory.

## Prerequisites

- Python 3.14+
- [uv](https://docs.astral.sh/uv/)

## Setup

```sh
git clone https://github.com/bittensor-church/nexus-poc.git
cd nexus-poc/bittensor-nexus-library
uv sync --group lint --group test
```

## Development

All commands must be run from the `bittensor-nexus-library` directory.

```sh
# Lint + format
uv run ruff check --fix && uv run ruff format

# Type checking
uv run basedpyright

# Tests
uv run pytest
```

If you prefer the template-style task runner, the same checks are exposed through `nox`:

```sh
uvx nox -s format
uvx nox -s lint
uvx nox -s test
```

## Validator wiring

`NexusValidator` discovers runtime components from `connect(source, sink)` calls.

## Project structure

```
nexus/
├── core/
│   ├── dsl/          # Flow DSL — nodes, pipes, flow definitions
│   └── runtime/      # Event bus, actors, context store, serialization
├── actors/           # Built-in actors (retry, REST, task-result splitting, S3, etc.)
├── examples/         # Examples of actor usage
└── utils/            # Shared utilities, including OpenRouter chat-completion helpers
tests/                # pytest suite (includes NexusTask wiring scaffolds in tests/nexus_task_test_setup.py)
```
