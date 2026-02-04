import copy
import random

import pytest

from nexus.core.dsl.nodes import Source
from nexus.core.runtime.context_store import ContextId, ContextStore, InMemoryContextStorePersistence
from nexus.core.runtime.context_store_types import (
    InvalidContextIdException,
)


def _random_payloads(rng: random.Random, count: int) -> list[dict[str, int]]:
    # FIXME: make the random payloads more complex and realistic, e.g. nested structures, lists, etc.
    payload = {}
    keys = []
    extra_idx = 0
    payloads = []
    for _ in range(count):
        action = rng.choice(["update", "add", "remove"])
        if action == "add" or not keys:
            key = f"extra_{extra_idx}"
            extra_idx += 1
            payload[key] = rng.randint(0, 10)
            keys.append(key)
        elif action == "update":
            key = rng.choice(keys)
            if key not in keys:
                keys.append(key)
            payload[key] = payload.get(key, 0) + rng.randint(1, 5)
        else:
            key = rng.choice(keys)
            keys.remove(key)
            payload.pop(key, None)
        payloads.append(copy.deepcopy(payload))
    return payloads


def _random_user_data(rng: random.Random, count: int) -> list[tuple[str, int]]:
    # FIXME: make the random user data more complex and realistic, e.g. nested structures, lists, etc.
    user_data: list[tuple[str, int]] = []
    for _ in range(count):
        key = f"key_{rng.randint(0, 10)}"
        value = rng.randint(0, 100)
        user_data.append((key, value))
    return user_data


def test_append_message_persists_and_recovers_payload():
    persistence = InMemoryContextStorePersistence()
    original_context_store = ContextStore.recover_from(persistence).context_store

    context = original_context_store.create_context()

    source = Source("payload-source")

    payloads = [
        {"count": 1, "items": [0, 1]},
        {"count": 2, "items": [0, 1, 2]},
        {"items": [2, 3]},
    ]
    for payload in payloads:
        context.append_message(source=source, payload=payload)

    entries = persistence.log_entries()
    assert len(entries) == len(payloads) + 1  # +1 for the initial ContextCreated entry

    another_context_store = ContextStore.recover_from(persistence).context_store
    recovered_context = another_context_store.get_context(context.id)
    assert recovered_context.payload == original_context_store.get_context(context.id).payload


def test_set_user_data_persists_and_recovers():
    persistence = InMemoryContextStorePersistence()
    original_context_store = ContextStore.recover_from(persistence).context_store

    context = original_context_store.create_context()

    context.set_user_data("count", 2)
    context.set_user_data("string", "asd")
    context.set_user_data("map", {"nested": True})

    entries = persistence.log_entries()
    assert len(entries) == 3 + 1  # +1 for the initial ContextCreated entry

    another_context_store = ContextStore.recover_from(persistence).context_store
    recovered_context = another_context_store.get_context(context.id)
    assert recovered_context.user_data == original_context_store.get_context(context.id).user_data


def test_get_context_returns_or_raises():
    persistence = InMemoryContextStorePersistence()
    context_store = ContextStore.recover_from(persistence).context_store

    context = context_store.create_context()

    assert context_store.get_context(context.id) == context

    with pytest.raises(InvalidContextIdException):
        context_store.get_context(ContextId("ctx-missing"))


def test_randomized_payloads_are_recoverable():
    # FIXME: fix randomness so that seed is random but test is deterministic
    rng = random.Random(1337)
    persistence = InMemoryContextStorePersistence()
    original_context_store = ContextStore.recover_from(persistence).context_store

    ctx = original_context_store.create_context()
    source = Source("random-source")

    payloads = _random_payloads(rng, 25)
    for payload in payloads:
        ctx.append_message(source=source, payload=payload)

    recovered = ContextStore.recover_from(persistence).context_store
    recovered_context = recovered.get_context(ctx.id)
    assert recovered_context.payload == original_context_store.get_context(ctx.id).payload


def test_randomized_user_data_is_recoverable():
    # FIXME: fix randomness so that seed is random but test is deterministic
    rng = random.Random(2024)
    persistence = InMemoryContextStorePersistence()
    original_context_store = ContextStore.recover_from(persistence).context_store

    ctx = original_context_store.create_context()

    random_user_data = _random_user_data(rng, 30)

    for set_user_data_event in random_user_data:
        ctx.set_user_data(set_user_data_event[0], set_user_data_event[1])

    recovered = ContextStore.recover_from(persistence).context_store
    recovered_context = recovered.get_context(ctx.id)
    assert recovered_context.user_data == original_context_store.get_context(ctx.id).user_data


def test_recover_rebuilds_messages():
    persistence = InMemoryContextStorePersistence()
    original_context_store = ContextStore.recover_from(persistence).context_store

    context_1 = original_context_store.create_context()
    context_2 = original_context_store.create_context(parents=(context_1.id,))

    # FIXME: add testcases for last message handling when the children context consumed the last messages
    # so we should replay them


def test_recover_multiple_contexts():
    persistence = InMemoryContextStorePersistence()
    original_context_store = ContextStore.recover_from(persistence).context_store

    ctx_a = original_context_store.create_context()
    ctx_b = original_context_store.create_context()

    source_a = Source("source-a")
    source_b = Source("source-b")

    ctx_a.append_message(source=source_a, payload=1)
    ctx_a.set_user_data("count", 2)
    ctx_b.append_message(source=source_b, payload=12)
    ctx_b.set_user_data("pound", 7)

    another_context_store = ContextStore.recover_from(persistence).context_store
    recovered_ctx_a = another_context_store.get_context(ctx_a.id)
    recovered_ctx_b = another_context_store.get_context(ctx_b.id)

    assert recovered_ctx_a.payload == original_context_store.get_context(ctx_a.id).payload
    assert recovered_ctx_a.user_data == original_context_store.get_context(ctx_a.id).user_data

    assert recovered_ctx_b.payload == original_context_store.get_context(ctx_b.id).payload
    assert recovered_ctx_b.user_data == original_context_store.get_context(ctx_b.id).user_data
