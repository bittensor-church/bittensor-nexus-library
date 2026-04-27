import copy
import pickle

import pytest

from nexus.v1 import ImmutableMap


def test_immutable_map_copies_input_and_blocks_item_assignment() -> None:
    original = {"alpha": 1}
    immutable = ImmutableMap(original)

    original["beta"] = 2

    assert dict(immutable) == {"alpha": 1}
    with pytest.raises(TypeError):
        immutable["gamma"] = 3  # type: ignore[index]


def test_immutable_map_blocks_top_level_mutation_through_internal_storage() -> None:
    immutable = ImmutableMap({"alpha": 1})

    with pytest.raises(AttributeError):
        immutable._items["beta"] = 2  # type: ignore[index]

    assert dict(immutable) == {"alpha": 1}


def test_immutable_map_blocks_rebinding_internal_storage_attribute() -> None:
    immutable = ImmutableMap({"alpha": 1})

    with pytest.raises(AttributeError):
        immutable._items = {"beta": 2}  # type: ignore[assignment]

    assert dict(immutable) == {"alpha": 1}


def test_immutable_map_blocks_rebinding_mangled_internal_storage_attribute() -> None:
    immutable = ImmutableMap({"alpha": 1})

    with pytest.raises(AttributeError):
        immutable._ImmutableMap__items = {"beta": 2}  # type: ignore[assignment]

    assert dict(immutable) == {"alpha": 1}


def test_immutable_map_accepts_iterable_of_tuples() -> None:
    immutable = ImmutableMap([("alpha", 1), ("beta", 2)])

    assert dict(immutable) == {"alpha": 1, "beta": 2}


def test_immutable_map_round_trips_through_deepcopy_and_pickle() -> None:
    immutable = ImmutableMap({"alpha": 1, "beta": 2})

    copied = copy.deepcopy(immutable)
    restored = pickle.loads(pickle.dumps(immutable))

    assert copied == immutable
    assert restored == immutable
    assert dict(copied) == {"alpha": 1, "beta": 2}
    assert dict(restored) == {"alpha": 1, "beta": 2}
