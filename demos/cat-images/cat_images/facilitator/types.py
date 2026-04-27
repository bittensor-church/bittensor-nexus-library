"""Shared primitive types and enums used across the facilitator package.
No model dependencies — everything here is a leaf in the import graph."""

from enum import StrEnum
from typing import NewType

JobId = NewType("JobId", str)
ValidatorHotkey = NewType("ValidatorHotkey", str)


class JobLiveness(StrEnum):
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"

    @property
    def is_terminal(self) -> bool:
        return self != JobLiveness.IN_PROGRESS
