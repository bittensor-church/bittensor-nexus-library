# Nexus

Opinionated event-driven actor framework for building Bittensor subnets. Thread-based concurrency, message-based flows.

## Prerequisites

- Python 3.14+
- [uv](https://docs.astral.sh/uv/)

## Setup

```sh
git clone https://github.com/bittensor-church/nexus-poc.git
cd nexus-poc/nexus-template
uv sync --all-extras
```

## Development

All commands must be run from the `nexus-template` directory.

```sh
# Lint + format
uv run ruff check --fix && uv run ruff format

# Type checking
uv run basedpyright

# Tests
uv run pytest
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
