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

### Added

- Opt-in idle-context expiry: `SubnetBuilder(context_ttl=...)` and `NexusValidator(settings, context_ttl=...)` complete and remove contexts that had no activity for longer than the TTL, so long-running validators no longer grow memory without bound. Activity is the context's creation, every log entry appended to it, and every release after use. The default `None` keeps every context as before. The TTL must exceed the longest gap between two uses of one context (retry delays, callback timeouts, gather windows). The cat-images demo uses a 3 hour TTL.
- `ContextStorePersistence.release_context(ctx)` is called for every expired context. It is a no-op by default; `InMemoryContextStorePersistence` drops the context's log entries.
- `ContextStore.expire_idle(ttl)` and an optional `clock` argument for `ContextStore.recover_from()`.

### Fixed

- The event bus drops a message for a missing or completed context with an error log instead of stopping its thread.
- `TimestamperNode` drops a queued output whose context no longer exists instead of failing every later flush, and `RetryStrategy` logs and abandons a retry whose context no longer exists.

### Changed

- `bittensor-pylon-client` is required as `>=2.3.1,<3` instead of exactly `2.3.1`, so validators can use newer compatible Pylon clients without overriding Nexus's dependency.
- `docs/nexus.md` lists setting `context_ttl` as a critical requirement for deployed validators, with guidance on sizing it.
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

### Fixed

- `TimestamperNode` no longer gets stuck when one output queued before the first `BlockBeat` cannot be timestamped. The flush now handles each queued output independently: a failing one is logged and dropped, the others are emitted, and the queue is always emptied. An output that arrives before the first `BlockBeat` without a start timestamp is rejected on arrival instead of blocking the queue's staleness checks.
- The `AsyncHttpNeuronCommunicator` timeout sweep no longer stops when emitting the timeout error for one request fails. The failure is logged with the request and context IDs, the remaining expired requests still get their timeout errors, and later requests keep timing out. A failing `PendingAsyncHttpRequestStore.pop_expired` call is likewise logged and retried on the next sweep.
- The `AsyncHttpNeuronCommunicator` sender loop no longer crashes when emitting the error for a failed send fails; the failure is logged and the sender keeps running.
- `AsyncHttpNeuronCommunicator` no longer silently drops a request whose stored input cannot be loaded. A failure (timeout, send failure, remote error) is emitted on the `error` source instead of `processed`; for a successful response the payload is discarded and an `InternalStateCorruptionException` is emitted on `error`. If the context itself is unknown, the outcome is logged and dropped.
