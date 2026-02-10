import copy
import datetime
import threading
import uuid
from abc import ABC, abstractmethod
from contextlib import AbstractContextManager, ExitStack, contextmanager
from dataclasses import dataclass
from typing import override, Iterable, Any, Iterator

import deepdiff
from deepdiff.serialization import json_loads, json_dumps

from .context_store_types import (
    LogEntryData,
    ContextId,
    MessageSent,
    UserDataChange,
    InvalidContextIdException,
    LogEntry,
    ChildContextCreated,
    ContextCreated,
    StepIdx,
)
from ..dsl.nodes import Source

type LastMessages = dict[ContextId, MessageSent]


class ContextStorePersistence(ABC):
    @abstractmethod
    def append_entry(self, ctx: ContextId, entry: LogEntryData) -> None:
        """
        Appends a new log entry to the store.
        """
        pass

    @abstractmethod
    def create_context(self, parents: tuple[ContextId, ...]) -> ContextId:
        """
        Creates a new context;
        If the parents is not empty, the new context is a child of the given parent contexts.
        """
        pass

    @abstractmethod
    def log_entries(self) -> Iterable[LogEntry]:
        """
        Returns all log entries stored in the persistence layer, ordered
        by their creation order within their respective contexts (i.e. by step idx).
        """
        pass


class ContextStoreLocks(ABC):
    """
    provides primitives for mutual exclusion on contexts, to ensure that only
    one entity can access a context at a time
    """
    @abstractmethod
    def register_context(self, ctx: ContextId) -> None:
        pass

    @abstractmethod
    def lock_context(self, ctx: ContextId) -> AbstractContextManager[None]:
        pass


class ThreadContextStoreLocks(ContextStoreLocks):
    __locks: dict[ContextId, threading.Lock]
    __registry_lock: threading.Lock

    def __init__(self) -> None:
        self.__locks = {}
        self.__registry_lock = threading.Lock()

    @override
    def register_context(self, ctx: ContextId) -> None:
        with self.__registry_lock:
            assert ctx not in self.__locks, f"Context {ctx} already registered in locks? It should have been created first."
            self.__locks[ctx] = threading.Lock()

    @override
    @contextmanager
    def lock_context(self, ctx: ContextId) -> Iterator[None]:
        with self.__registry_lock:
            context_lock = self.__locks.get(ctx, None)
        if context_lock is None:
            raise InvalidContextIdException(f"Context lock for {ctx} not found")

        context_lock.acquire()
        try:
            yield
        finally:
            context_lock.release()


@dataclass(frozen=True)
class RecoveredContextStore:
    context_store: ContextStore
    last_messages: LastMessages


def _assert_recovery(old_value, delta_json, new_value):
    # not sure if we should have that assert in production code...

    delta = deepdiff.Delta(delta_json, deserializer=json_loads)
    recovered_value = old_value + delta
    diff = deepdiff.DeepDiff(recovered_value, new_value)
    assert len(diff) == 0, (
        f"delta application did not recover the new value? recovered value: {recovered_value} != new value: {new_value};\n"
        "old value: {old_value}\napplied delta = {delta_json}\n"
        "detected differences: {diff}"
    )


class Context:
    """
    Represents a specific context of execution in the system.
    Special care is taken to ensure that the context's payload and user data
    are only modified through the interface methods, as changing to the context
    should always be accompanied by appending the corresponding log entry
    to the persistence layer.

    A Context may only be used by a single thread at a time, and is not thread safe.
    Mutual exclusion is provided by the ContextStore methods, which provide
    Context and it's ownership (via context managers)
    """

    _id: ContextId
    _payload: Any
    _user_data: dict[str, Any]

    _context_store: ContextStore

    @property
    def id(self):
        return self._id

    @property
    def payload(self):
        return copy.deepcopy(self._payload)

    @property
    def user_data(self):
        return copy.deepcopy(self._user_data)

    def __init__(self, _id: ContextId, payload: Any, user_data: dict[str, Any], context_store: ContextStore) -> None:
        self._id = _id
        self._payload = payload
        self._user_data = user_data
        self._context_store = context_store

    def append_message[T](self, source: Source[T], payload: T):
        payload_delta = deepdiff.Delta(deepdiff.DeepDiff(self._payload, payload), serializer=json_dumps)
        payload_delta_json = payload_delta.dumps()
        self._context_store._append_entry(
            self._id, MessageSent(source=source.id, payload_delta_json=payload_delta_json)
        )

        _assert_recovery(self._payload, payload_delta_json, payload)

        self._payload = copy.deepcopy(payload)

    def set_user_data(self, key: str, value: Any) -> None:
        old_value = self._user_data.get(key, None)
        delta = deepdiff.Delta(deepdiff.DeepDiff(old_value, value), serializer=json_dumps)
        value_data_json = delta.dumps()
        self._context_store._append_entry(self._id, UserDataChange(key=key, value_delta_json=value_data_json))

        _assert_recovery(old_value, value_data_json, value)

        self._user_data[key] = copy.deepcopy(value)


class ContextStore:
    """
    Stores and manages contexts and their associated log entries.

    Each context is associated with a linear sequence of log entries that represent the history
    of that context, including its creation, messages sent, user data changes, etc.

    Multiple context can have parent-children relationships. When a context is created with parent contexts,
    a log entry is appended to each parent context indicating the creation of the child context. Also,
    the child context is initialized with a log entry that indicates its parent contexts.
    This allows us to reconstruct the context tree and the relationships between contexts during recovery.
    """

    __persistence: ContextStorePersistence
    __contexts: dict[ContextId, Context]
    __locks: ContextStoreLocks

    @classmethod
    def recover_from(cls: type[ContextStore], persistence: ContextStorePersistence) -> RecoveredContextStore:
        contexts: dict[ContextId, Context] = {}
        last_messages: LastMessages = {}

        steps: dict[ContextId, StepIdx] = {}

        context_store = ContextStore(token="factory_method")

        for log_entry in persistence.log_entries():
            ctx = log_entry.ctx
            previous_step = steps.get(ctx, None)
            assert log_entry.step_idx == StepIdx(0) or log_entry.step_idx == previous_step + 1, (
                f"log entry step idx out of order for context {ctx}: expected {log_entry.step_idx} to be after {previous_step}"
            )
            steps[ctx] = log_entry.step_idx
            match log_entry.data:
                case ContextCreated():
                    context = Context(_id=ctx, payload=None, user_data={}, context_store=context_store)
                    contexts[ctx] = context
                case ChildContextCreated():
                    pass
                case MessageSent(payload_delta_json=payload_delta_json) as message:
                    context = contexts[ctx]
                    context._payload += deepdiff.Delta(payload_delta_json, deserializer=json_loads)
                    last_messages[ctx] = message
                case UserDataChange(key=key, value_delta_json=value_delta_json):
                    context = contexts[ctx]
                    context._user_data[key] = context._user_data.get(key, None) + deepdiff.Delta(
                        value_delta_json, deserializer=json_loads
                    )

        # true intialization of the ContextStore
        context_store.__persistence = persistence
        context_store.__contexts = contexts
        context_store.__locks = ThreadContextStoreLocks()
        for context_id in contexts:
            context_store.__locks.register_context(context_id)

        return RecoveredContextStore(context_store, last_messages)

    def __init__(self, token="intialize_using_factory_methods_not_directly") -> None:
        assert token == "factory_method", "ContextStore should be initialized using factory methods, not directly."

    @contextmanager
    def create_context(self, *, parents: tuple[ContextId, ...] = ()) -> Iterator[Context]:
        new_context_id = self.__persistence.create_context(parents)
        context = Context(_id=new_context_id, payload=None, user_data={}, context_store=self)
        self.__contexts[new_context_id] = context
        self.__locks.register_context(new_context_id)

        with self.__locks.lock_context(new_context_id):
            yield context

    @contextmanager
    def get_context(self, ctx: ContextId) -> Iterator[Context]:
        with self.__locks.lock_context(ctx):
            if ctx not in self.__contexts:
                raise InvalidContextIdException(f"Context {ctx} not found")

            yield self.__contexts[ctx]

    def _append_entry(self, ctx: ContextId, entry: LogEntryData) -> None:
        assert ctx in self.__contexts, f"Context {ctx} not found in store? It should have been created first."
        self.__persistence.append_entry(ctx, entry)


class InMemoryContextStorePersistence(ContextStorePersistence):
    """
    An in-memory implementation of ContextStorePersistence for testing and development purposes.
    """

    __data: dict[ContextId, list[LogEntry]]  # context_id to log entries mapping
    __context_tree: dict[ContextId, list[ContextId]]  # parent to children mapping

    def __init__(self) -> None:
        self.__data = {}
        self.__context_tree = {}

    @override
    def append_entry(self, ctx: ContextId, entry: LogEntryData) -> None:
        assert ctx in self.__data, f"Context {ctx} does not exist? It should have been created first."
        log_entry = LogEntry(
            ctx=ctx,
            step_idx=StepIdx(len(self.__data[ctx])),
            creation_time=datetime.datetime.now(tz=datetime.timezone.utc),
            data=entry,
        )
        self.__data[ctx].append(log_entry)

    @override
    def create_context(self, parents: tuple[ContextId, ...]) -> ContextId:
        """
        Creates a new context that has a parent-child relationship with the given parent contexts.
        """
        for parent_ctx in parents:
            assert parent_ctx in self.__context_tree, (
                f"Context {parent_ctx} does not exist? It should have been created first."
            )
            assert parent_ctx in self.__data, (
                f"Context {parent_ctx} data does not exist? It should have been created first."
            )

        child_ctx = ContextId(str(uuid.uuid7()))
        context_creation_time = datetime.datetime.now(tz=datetime.timezone.utc)

        for parent_ctx in parents:
            log_entry = LogEntry(
                ctx=parent_ctx,
                step_idx=StepIdx(len(self.__data[parent_ctx])),
                creation_time=context_creation_time,
                data=ChildContextCreated(child_ctx=child_ctx),
            )
            self.__data[parent_ctx].append(log_entry)
            self.__context_tree[parent_ctx].append(child_ctx)

        child_log_entry = LogEntry(
            ctx=child_ctx,
            step_idx=StepIdx(0),
            creation_time=context_creation_time,
            data=ContextCreated(parents=parents),
        )

        self.__data[child_ctx] = [child_log_entry]
        self.__context_tree[child_ctx] = []

        return child_ctx

    @override
    def log_entries(self) -> tuple[LogEntry, ...]:
        all_entries = []
        for log_entries in self.__data.values():
            all_entries.extend(log_entries)
        all_entries.sort(key=lambda entry: entry.step_idx)
        return tuple(all_entries)
