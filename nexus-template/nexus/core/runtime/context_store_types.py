import datetime
from dataclasses import dataclass
from typing import NewType, TYPE_CHECKING

from pydantic import BaseModel

from ..dsl.nodes import SourceId

if TYPE_CHECKING:
    from .context_store import Context

ContextId = NewType("ContextId", str)
StepIdx = NewType("StepIdx", int)


class InvalidContextIdException(Exception):
    pass


class InvalidLogEntryIdException(Exception):
    pass


class MessageSent(BaseModel):
    """
    Log entry representing a message sent event.
    """

    source: SourceId
    payload_delta_json: str


class UserDataChange(BaseModel):
    """
    Log entry representing a message sent event.
    """

    key: str
    value_delta_json: str


class ChildContextCreated(BaseModel):
    """
    Log entry representing the creation of a child context by the current context.
    """

    child_ctx: ContextId


class ContextCreated(BaseModel):
    """
    Log entry representing the creation of a context, possibly by multiple parent contexts.
    """

    parents: tuple[ContextId, ...]


# union for exhaustive type checking
type LogEntryData = MessageSent | UserDataChange | ChildContextCreated | ContextCreated


LogEntryId = NewType("LogEntryId", int)


class LogEntry(BaseModel):
    ctx: ContextId
    step_idx: StepIdx
    creation_time: datetime.datetime
    data: LogEntryData
