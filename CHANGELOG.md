# Changelog

## Unreleased

### Breaking changes

- Replaced ambiguous per-source sink sets with explicit `Targets(primary=..., taps=...)` in the `nexus.v1` API.
  A primary keeps the existing context; every declared tap, including a sole tap, receives a distinct child context.
  Taps-only wiring does not dispatch the parent context.
- `Flow.then(a, b)` and iterable named routes such as `then(ok=[a, b])` are no longer accepted. Migrate them to
  `then(a, taps=[b])`, `then(taps=[a, b])`, or `then(ok=Targets(primary=a, taps=[b]))` according to the intended roles.
- `Pipes[source]` now returns `Targets[Sink]` instead of a raw sink set. Read `.primary` and `.taps` explicitly.
- `Context.copy_parent_context_snapshots()` now returns an
  `ImmutableMap[ContextId, ParentContextSnapshot]` instead of a tuple. Select snapshots by parent context ID rather
  than position; mapping iteration order is unspecified. Duplicate parent IDs produce one relationship and snapshot.
- Contexts are now released once no queued or in-progress message refers to them. Code that keeps a context ID
  beyond the handler that received it (timers, pending requests, queues, inspecting a context after its flow ended)
  must call `ContextStore.retain(ctx_id)` while handling that context and emit `ReleaseContextEvent(ctx_id)` after its
  last message for it. Accessing a released context raises `InvalidContextIdException`. A new context that nothing
  references is released after the bus dispatches its first message; retain it before emitting more than once.

### Fixed

- Contexts and their in-memory log entries are no longer kept forever. Previously every message source emission and
  every tap child context stayed in the `ContextStore` and `InMemoryContextStorePersistence` until restart, so a
  validator with a periodic clock grew its memory without bound.

### Added

- `ContextStore.retain()` and `ReleaseContextEvent` to keep a context alive beyond its handler, and
  `ContextStore.live_context_count` to observe the number of unreleased contexts.
- `ContextStorePersistence.release_context()` hook, called after a context is released. The default keeps the
  entries; `InMemoryContextStorePersistence` drops them.

### Changed

- The event bus takes a context reference for every delivery, and actors release it after handling the message and
  queueing what it produced, including when the handler raised. Released contexts end with a `ContextCompleted`
  entry, so recovery skips them.
- `RetryStrategy`, `TimestamperNode` and `AsyncHttpNeuronCommunicator` retain the contexts they wait on.
- The event bus logs and drops a message for an unknown or released context, and logs an unbalanced release, instead
  of stopping its loop.

- Updated `bittensor-pylon-client` to 2.3.1 and the cat-images Pylon service images to 2.3.4.
  When overriding `PYLON_BITTENSOR_NETWORK`, also set `PYLON_BITTENSOR_ARCHIVE_NETWORK` for the same chain;
  the demo localnet examples configure both endpoints.
- `NexusValidator.connect` now accepts `connect(source, primary=None, *, taps=...)`. Existing two-argument calls remain
  primary connections. Subnet-clock consumers, including Nexus Tasks and the cat-images weight-setting trigger, are
  connected as taps.
- Target declarations are merged centrally: duplicate taps and repeated identical primaries are idempotent, while
  conflicting primaries or a sink assigned both roles raise `FlowMisconfiguredException`.
- All tap child contexts and parent snapshots are created before dispatch begins, so primary processing cannot change
  what sibling taps inherit from the emission.
