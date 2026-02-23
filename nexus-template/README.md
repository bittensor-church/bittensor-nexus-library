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
uv run pytest -q --tb=line -r f
```

## Project structure

```
nexus/
├── core/
│   ├── dsl/          # Flow DSL — nodes, pipes, flow definitions
│   └── runtime/      # Event bus, actors, context store, serialization
├── actors/           # Built-in actors (scheduler, retry, REST, S3, etc.)
├── examples/         # Example actors for reference
└── utils/            # Shared utilities
tests/                # pytest suite
```