# Nexus

Opinionated event-driven actor framework for building Bittensor subnets. Thread-based concurrency, message-based flows.

## Prerequisites

- Python 3.14+
- [uv](https://docs.astral.sh/uv/)

## Setup

```sh
git clone https://github.com/bittensor-church/nexus-poc.git
cd nexus-poc
uv sync --all-groups
```

## Development

Full-project QA is run from the repository root through nox.

```sh
# Root lint, formatting check, type check, docs/shell checks
uvx nox -vs lint

# Root tests
uvx nox -vs test-3.14

# cat-images demo lint, formatting check, type check
uvx nox -vs cat-images-lint

# cat-images demo tests
uvx nox -vs cat-images-test
```

## mTLS configuration

The default `EnvAsyncPylonClientProvider` builds the async Pylon client used for validator→miner HTTP
communication. mTLS is enabled when both cert env vars are present; plain HTTP is used otherwise.

> **Note:** mTLS requires the validator to be a registered neuron with its public key on-chain. Cert
> generation and verification are tied to the validator's Pylon identity (`VALIDATOR_PYLON_IDENTITY_NAME`
> and `VALIDATOR_PYLON_IDENTITY_TOKEN`), so mTLS will not work without valid on-chain registration.

| Variable | Description |
|---|---|
| `VALIDATOR_MTLS_CERT_PATH` | Path to the validator TLS certificate |
| `VALIDATOR_MTLS_KEY_PATH` | Path to the validator TLS private key |
| `VALIDATOR_NEURONS_FILE` | Path to a local neurons JSON file — bypasses chain lookup, disables mTLS; intended for local dev only |
| `VALIDATOR_NEURON_CONNECTION_KEEPALIVE_EXPIRY` | Seconds an idle connection to a neuron stays pooled before being closed (default 60) |

## Validator wiring

`NexusValidator` discovers runtime components from `connect(source, sink)` calls.

## Public API

Import Nexus public interfaces from the versioned API package:

```py
from nexus.v1 import Flow, NexusTask, NexusValidator, Source
```

Implementation modules live under `nexus._internal` and are not public API. Do not import from legacy paths such as
`nexus.actors`, `nexus.core`, `nexus.utils`, or `nexus.nexus_validator`.

## Project structure

```
src/nexus/
├── v1/                # Public versioned API facade
└── _internal/         # Implementation modules
    ├── core/
    │   ├── dsl/       # Flow DSL: nodes, pipes, flow definitions
    │   └── runtime/   # Event bus, actors, context store, serialization
    ├── actors/        # Built-in actors (retry, REST, task-result splitting, S3, etc.)
    ├── examples/      # Examples of actor usage
    └── utils/         # Shared utilities, including OpenRouter chat-completion helpers
demos/cat-images/      # Demo subnet package and Docker/localnet tooling
tests/                 # pytest suite (includes NexusTask wiring scaffolds in tests/nexus_task_test_setup.py)
```
