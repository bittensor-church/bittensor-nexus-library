from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from types import MappingProxyType
from typing import Self


class ImmutableMap[K, V](Mapping[K, V]):
    __slots__ = ("__items",)
    __items: Mapping[K, V]

    def __init__(self, items: Mapping[K, V] | Iterable[tuple[K, V]]) -> None:
        object.__setattr__(self, "_ImmutableMap__items", MappingProxyType(dict(items)))

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError(f"{type(self).__name__} does not support attribute assignment")

    def __getitem__(self, key: K) -> V:
        return self.__items[key]

    def __iter__(self) -> Iterator[K]:
        return iter(self.__items)

    def __len__(self) -> int:
        return len(self.__items)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.__items!r})"

    def __reduce__(self) -> tuple[type[Self], tuple[dict[K, V]]]:
        return type(self), (dict(self.__items),)
