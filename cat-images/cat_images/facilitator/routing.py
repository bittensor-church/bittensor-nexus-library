import random
from collections.abc import Sequence
from typing import Protocol

from cat_images.facilitator.models import CatificationRequest, RegisteredValidator
from cat_images.facilitator.stores import ValidatorStore


class RoutingStrategy(Protocol):
    def pick(
        self,
        request: CatificationRequest,
        validators: Sequence[RegisteredValidator],
    ) -> RegisteredValidator | None: ...


class RandomStrategy:
    def pick(
        self,
        request: CatificationRequest,
        validators: Sequence[RegisteredValidator],
    ) -> RegisteredValidator | None:
        return random.choice(validators) if validators else None


class ValidatorRouter:
    def __init__(self, strategy: RoutingStrategy, validator_store: ValidatorStore) -> None:
        self._strategy = strategy
        self._validator_store = validator_store

    def select(self, request: CatificationRequest) -> RegisteredValidator | None:
        available = self._validator_store.list_available()
        if not available:
            return None
        return self._strategy.pick(request, available)
