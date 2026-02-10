import copy
import datetime
import logging
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
    ContextCompleted,
    StepIdx,
)
from ..dsl.nodes import Source
from ... import get_logger

type LastMessages = dict[ContextId, MessageSent]


logger: logging.Logger = get_logger(__name__)

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
    __locks: dict[ContextId, threading.RLock]
    __registry_lock: threading.Lock

    def __init__(self) -> None:
        self.__locks = {}
        self.__registry_lock = threading.Lock()

    @override
    def register_context(self, ctx: ContextId) -> None:
        with self.__registry_lock:
            assert ctx not in self.__locks, f"Context {ctx} already registered in locks? It should have been created first."
            self.__locks[ctx] = threading.RLock()

    @override
    @contextmanager
    def lock_context(self, ctx: ContextId) -> Iterator[None]:
        with self.__registry_lock:
            context_lock = self.__locks.get(ctx, None)
        if context_lock is None:
            raise InvalidContextIdException(f"Context lock for {ctx} not found")

        # try-lock; it should succeed immediately if the framework is correctly ensuring that there
        # is no concurrent processing of the same context.
        # We use a try-lock here to pro-actively detect any bugs in the framework that may lead to
        # concurrent processing of the same context, deadlocks etc.
        acquired = context_lock.acquire(blocking=False)
        if not acquired:
            logger.error(f"Context {ctx} is already locked?; this This is unexpected and may suggest a bug. Attempting to acquire lock in a blocking way...")
            context_lock.acquire()
        try:
            logger.info(f"Context {ctx} locked for processing.")
            yield
        finally:
            context_lock.release()
            logger.info(f"Context {ctx} released from processing.")


@dataclass(frozen=True)
class RecoveredContextStore:
    context_store: ContextStore
    last_messages: LastMessages


class ContextCompletedException(Exception):
    pass


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
    See also ContextStore for a more high-level overview of the context concept and its management.

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
    _is_completed: bool

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

    @property
    def is_completed(self) -> bool:
        return self._is_completed

    def __init__(
        self,
        _id: ContextId,
        payload: Any,
        user_data: dict[str, Any],
        context_store: ContextStore,
    ) -> None:
        self._id = _id
        self._payload = payload
        self._user_data = user_data
        self._is_completed = False
        self._context_store = context_store

    def append_message[T](self, source: Source[T], payload: T):
        self._assert_mutable()
        payload_delta = deepdiff.Delta(deepdiff.DeepDiff(self._payload, payload), serializer=json_dumps)
        payload_delta_json = payload_delta.dumps()
        self._context_store._append_entry(
            self._id, MessageSent(source=source.id, payload_delta_json=payload_delta_json)
        )

        _assert_recovery(self._payload, payload_delta_json, payload)

        self._payload = copy.deepcopy(payload)

    def set_user_data(self, key: str, value: Any) -> None:
        self._assert_mutable()
        old_value = self._user_data.get(key, None)
        delta = deepdiff.Delta(deepdiff.DeepDiff(old_value, value), serializer=json_dumps)
        value_data_json = delta.dumps()
        self._context_store._append_entry(self._id, UserDataChange(key=key, value_delta_json=value_data_json))

        _assert_recovery(old_value, value_data_json, value)

        self._user_data[key] = copy.deepcopy(value)

    def complete(self) -> None:
        self._assert_mutable()
        self._context_store._append_entry(self._id, ContextCompleted())
        self._is_completed = True

    def _assert_mutable(self) -> None:
        if self._is_completed:
            raise ContextCompletedException(f"Context {self._id} is completed and can no longer be mutated.")


class ContextStore:
    """
    Stores and manages contexts and their associated log entries.

    Each context is associated with a linear sequence of log entries that represent the history
    of that context, including its creation, messages sent, user data changes, etc.

    A context log always starts with a ContextCreated entry and ends with a ContextCompleted entry.

    A context can scatter processing across multiple contexts by creating child contexts (e.g. consider
    a use case where a child context is created to execute validation, and another child context continues
    to return a response to the user;

    Multiple parent context can be gathered together to create a child context that represents
    the combined processing of all parent contexts. E.g. consider a use case where multiple
    user requests are batched together to be processed as a single unit, and a child context
    is created to represent the processing of the batch. Later on, the child context can
    be scattered again to return individual responses to each individual user request.

    The scatter-gather operations are realized by contexts having parent-children relationships.
    When a context is created with parent contexts, a log entry is appended to each parent
    context indicating the creation of the child context. Also, the child context is initialized
    with a log entry that indicates its parent contexts.

    Conversely, it is also possible that a single context is a parent of multiple child contexts.

    This allows us to reconstruct the context tree and the relationships between contexts during
    recovery or audit.

    A Context must be "owned" for processing. For that purpose ContextStoreLocks are used to
    ensure mutual exclusion on contexts during processing, and to prevent concurrent modifications
    to the same context.
    These lock should NOT be strictly necessary as the framework is supposed to make sure
    there can not be concurrent processing of the same context. They are an extra safety
    measure to prevent data corruption and to pro-actively signal any bugs in the framework
    that may lead to concurrent processing of the same context.

    Context ownership rules:
    - To create a new context, you must have ownership of all parent contexts (if any).
    - To access an existing context, you must have ownership of that context.
    - Currently, ownership is automatically acquired and released for the duration of
      processing of a specific message.
    - You may want to retain ownership of a context across multiple messages, but that is
      not currently supported. E.g. for the case
      of creating a context with multiple parents (for which you should own all parents),
      the parents get locked during create_context instead of being owned across many
      messages coming from multiple parent contexts; This is sub-optimal, but good enough
      for the time being.

      Please consider these rules especially whenever you want to scatter or gather
      processing across contexts, and when you want to access contexts in your code.
    """

    __persistence: ContextStorePersistence
    __contexts: dict[ContextId, Context]
    __locks: ContextStoreLocks

    @classmethod
    def recover_from(cls: type[ContextStore], persistence: ContextStorePersistence) -> RecoveredContextStore:
        contexts: dict[ContextId, Context] = {}
        last_messages: LastMessages = {}
        completed_contexts: set[ContextId] = set()

        steps: dict[ContextId, StepIdx] = {}

        context_store = ContextStore(token="factory_method")

        for log_entry in persistence.log_entries():
            ctx = log_entry.ctx
            previous_step = steps.get(ctx, None)
            assert log_entry.step_idx == StepIdx(0) or log_entry.step_idx == previous_step + 1, (
                f"log entry step idx out of order for context {ctx}: expected {log_entry.step_idx} to be after {previous_step}"
            )
            steps[ctx] = log_entry.step_idx
            assert ctx not in completed_contexts, (
                f"Log entry {log_entry.step_idx} for context {ctx} appears after completion."
            )
            match log_entry.data:
                case ContextCreated():
                    assert ctx not in contexts, f"Context {ctx} already exists during recovery."
                    context = Context(_id=ctx, payload=None, user_data={}, context_store=context_store)
                    contexts[ctx] = context
                case ChildContextCreated():
                    pass
                case MessageSent(payload_delta_json=payload_delta_json) as message:
                    context = contexts.get(ctx, None)
                    assert context is not None, f"MessageSent for missing context {ctx} during recovery."
                    context._payload += deepdiff.Delta(payload_delta_json, deserializer=json_loads)
                    last_messages[ctx] = message
                case UserDataChange(key=key, value_delta_json=value_delta_json):
                    context = contexts.get(ctx, None)
                    assert context is not None, f"UserDataChange for missing context {ctx} during recovery."
                    context._user_data[key] = context._user_data.get(key, None) + deepdiff.Delta(
                        value_delta_json, deserializer=json_loads
                    )
                case ContextCompleted():
                    context = contexts.pop(ctx, None)
                    assert context is not None, f"ContextCompleted for missing context {ctx} during recovery."
                    context._is_completed = True
                    completed_contexts.add(ctx)
                    last_messages.pop(ctx, None)

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
        """
        creates a new Context with the given parent contexts, and yields it with a context manager
        that ensures mutual exclusion on the new context.

        Parents get locked for the duration of the context creation to ensure that no new log entries
        are appended to them during the creation process
        """
        parent_ids = tuple(dict.fromkeys(parents))
        with self._lock_contexts(parent_ids):
            for parent_id in parent_ids:
                parent_context = self.__contexts.get(parent_id, None)
                if parent_context is None:
                    raise InvalidContextIdException(f"Parent context {parent_id} not found")
                if parent_context.is_completed:
                    raise ContextCompletedException(
                        f"Parent context {parent_id} is completed and cannot create child contexts."
                    )

            new_context_id = self.__persistence.create_context(parent_ids)
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
        context = self.__contexts.get(ctx, None)
        assert context is not None, f"Context {ctx} not found in store? It should have been created first."
        if context.is_completed:
            raise ContextCompletedException(f"Context {ctx} is completed and cannot be mutated.")
        self.__persistence.append_entry(ctx, entry)

    @contextmanager
    def _lock_contexts(self, context_ids: Iterable[ContextId]) -> Iterator[None]:
        with ExitStack() as stack:
            for context_id in sorted(set(context_ids)):
                stack.enter_context(self.__locks.lock_context(context_id))
            yield


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
