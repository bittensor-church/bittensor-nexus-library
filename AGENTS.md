# Nexus

Opinionated event-driven actor framework for building Bittensor subnets. Python, strict typing, thread-based
concurrency, and message-based IPC.

## Tech

- Python 3.14
- uv as package manager (no pip)
- pytest, basedpyright
- ruff for linting and formatting
- litestar for HTTP endpoints (https://docs.litestar.dev/latest/)
- pylon for subtensor communication (https://github.com/bittensor-church/bittensor-pylon) - no `bittensor` dependency

## QA tools

These tools are configured to comply with project conventions.
They MUST be run within the `nexus-template` or `cat-images` directory.
ALWAYS use these instead of manual checks.

```sh
uv run ruff check --fix && uv run ruff format   # lint + format
uv run basedpyright                             # type checking
uv run pytest -q --tb=line -r f                 # tests
```

## Architecture

The framework is built around the actor pattern. Actors are independent processing units connected into pipelines via
typed message passing. Each actor runs in its own thread with queue-based IPC — no shared mutable state, no async.
Threads isolate actors from each other so one bad actor doesn't completely stall the rest of the pipeline, and actor
code stays linear and easy to debug.

## Core Concepts

**Nodes and Actors** — a Node is a setup-time declaration: it describes an actor's configuration and acts as its factory
(though it only ever creates one instance). An Actor is the runtime counterpart — a thread that does the actual work.
The
actor holds a reference back to its node to read config. Think of nodes as the blueprint, actors as the running process.

**Sources and Sinks** — each actor has named sources (outputs) and sinks (inputs), similar to network ports. A Source or
Sink is fundamentally just a typed identifier. Messages are paired with their source/sink to denote where they came from
and where they're going. Actors receive messages on sinks and emit messages from sources.

**Pipes** — connect a source of one actor to a sink of another, forming the processing graph. The EventBus uses these
connections to route messages.

**Contexts** — every origin message entering a pipeline gets its own Context. As the message is transformed and passed
between actors, it carries the same context throughout the entire flow. Contexts include an arbitrary data bag that
actors
can use to store persistent per-flow information. Contexts survive restarts — they are persisted and reloaded. The
framework diffs context state to enable tracing and event replayability.
Contexts are always linear. If there is a scatter point during processing multiple children contexts should be created. Conversely, when there's a gather point where multiple contexts are aggregated a new child context with multiple parents should be created.
**Naming matters** — node and actor IDs/names are used for identification across persistence, tracing, and routing. Pick
descriptive, stable, unique names.

Always use `uv run`, never bare `python` or `pip`.

## Coding guidelines

- All imports MUST go at the top of a file. No inline imports.
- Public consumers MUST import Nexus interfaces from `nexus.v1`. Implementation modules live under
  `nexus._internal`; do not import from legacy public paths such as `nexus.actors`, `nexus.core`, `nexus.utils`, or
  `nexus.nexus_validator`.
- Prefer short and concise code. Avoid deeply nested code.
- Code MUST be well-typed. Prefer typed structures over dictionaries.
- Be strict with domain types: avoid bare `str`/`int`/`float` for domain values when a stronger type exists.
- For durations use `datetime.timedelta` (not raw numeric seconds) unless there is a strong reason not to.
- Introduce `NewType` or small typed wrappers for semantically distinct values (paths, ports, IDs, timeouts) to prevent argument mix-ups and make APIs self-explanatory.
- Use short and descriptive variable, function, and class names.
- Comments MUST NOT reiterate what the code does. If the code is descriptive enough - skip comments.
- DO write comments that explain non-obvious code, gotchas.
- Do NOT write overly defensive code, NEVER use hasattr/getattr to inspect objects that are well typed.
- Do NOT double-check for things that are already handled by the project's QA tools - use the tools instead.
- Do NOT leave dead code in the codebase.
- You are allowed to restructure code. Prefer this over introducing workarounds.
- Do NOT use assertions in production code for runtime invariants. Raise a specific exception instead.
- `InternalFrameworkException` is a common default for invariant breaches, but choose context-specific exceptions when appropriate (for example `InternalStateCorruptionException`, `FlowMisconfiguredException`, `ActorMisconfiguredException`, `SubnetMisconfiguredException`).

## Documentation guidelines

- You MUST proactively update documentation for whatever you are working on.
- ALWAYS keep the project's README, AGENTS.md, code comments, and docstrings up to date.
- ALWAYS create and keep up-to-date high-level documentation for "public" classes, functions, and modules.
