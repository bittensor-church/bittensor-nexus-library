import copy
import datetime
import logging
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Generator, Iterable
from contextlib import AbstractContextManager, ExitStack, contextmanager
from dataclasses import dataclass
from typing import Any, cast, override

import deepdiff

from ... import get_logger
from ...utils.exceptions import InternalFrameworkException, InternalStateCorruptionException
from ...utils.immutable_map import ImmutableMap
from ..dsl.nodes import Source
from .context_store_types import (
    ChildContextCreated,
    ContextCompleted,
    ContextCreated,
    ContextDataInitialized,
    ContextId,
    InvalidContextIdException,
    LogEntry,
    LogEntryData,
    MessageSent,
    ParentContextSnapshot,
    StepIdx,
    UserDataChange,
    UserNote,
)
from .serialization import unsafe_pickle_load

type LastMessages = dict[ContextId, MessageSent]
type AnyPayload = Any
type AnyUserData = Any
type MonotonicClock = Callable[[], datetime.timedelta]

logger: logging.Logger = get_logger(__name__)

DELTA_DESERIALIZER = unsafe_pickle_load


def monotonic_now() -> datetime.timedelta:
    """Time on the monotonic clock, measured from an arbitrary reference point."""
    return datetime.timedelta(seconds=time.monotonic())


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

    def release_context(self, ctx: ContextId) -> None:  # noqa: B027
        """
        Called after the ContextStore expired a context and appended its ContextCompleted entry.

        The default keeps all entries; recovery skips completed contexts anyway. Implementations may drop or archive
        them. Children of the context may still be live; they keep their own parent snapshots. A failed call is retried
        on a later expiry sweep, so implementations should tolerate repeated calls.
        """


class ContextStoreLocks(ABC):
    """
    provides primitives for mutual exclusion on contexts, to ensure that only
    one entity can access a context at a time
    """

    @abstractmethod
    def register_context(self, ctx: ContextId) -> None:
        pass

    @abstractmethod
    def unregister_context(self, ctx: ContextId) -> None:
        pass

    @abstractmethod
    def lock_context(self, ctx: ContextId) -> AbstractContextManager[None]:
        pass

    @abstractmethod
    def try_lock_context(self, ctx: ContextId) -> AbstractContextManager[bool]:
        """Locks the context if it is free, without blocking; yields whether the lock was acquired."""
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
            if ctx in self.__locks:
                raise InternalFrameworkException(
                    f"Context {ctx} already registered in locks? It should have been created first."
                )
            self.__locks[ctx] = threading.RLock()

    @override
    def unregister_context(self, ctx: ContextId) -> None:
        with self.__registry_lock:
            if self.__locks.pop(ctx, None) is None:
                raise InternalFrameworkException(f"Context {ctx} is not registered in locks.")

    @override
    @contextmanager
    def try_lock_context(self, ctx: ContextId) -> Generator[bool]:
        context_lock = self.__lock_for(ctx)
        acquired = context_lock.acquire(blocking=False)
        try:
            yield acquired
        finally:
            if acquired:
                context_lock.release()

    @override
    @contextmanager
    def lock_context(self, ctx: ContextId) -> Generator[None]:
        context_lock = self.__lock_for(ctx)

        # try-lock; it should succeed immediately if the framework is correctly ensuring that there
        # is no concurrent processing of the same context.
        # We use a try-lock here to pro-actively detect any bugs in the framework that may lead to
        # concurrent processing of the same context, deadlocks etc.
        acquired = context_lock.acquire(blocking=False)
        if not acquired:
            logger.error(
                f"Context {ctx} is already locked?; this This is unexpected and may suggest a bug. "
                "Attempting to acquire lock in a blocking way..."
            )
            context_lock.acquire()
        try:
            logger.debug(f"Context {ctx} locked for processing.")
            yield
        finally:
            context_lock.release()
            logger.debug(f"Context {ctx} released from processing.")

    def __lock_for(self, ctx: ContextId) -> threading.RLock:
        with self.__registry_lock:
            context_lock = self.__locks.get(ctx, None)
        if context_lock is None:
            raise InvalidContextIdException(f"Context lock for {ctx} not found")
        return context_lock


@dataclass(frozen=True)
class RecoveredContextStore:
    context_store: ContextStore
    last_messages: LastMessages


class ContextCompletedException(Exception):
    pass


def _assert_recovery(old_value: Any, delta: bytes, new_value: Any):
    # Not sure if we should enforce this check in production code...

    parsed_delta = deepdiff.Delta(delta, deserializer=DELTA_DESERIALIZER)
    recovered_value = old_value + parsed_delta
    diff = deepdiff.DeepDiff(recovered_value, new_value)
    if len(diff) != 0:
        raise InternalFrameworkException(
            "delta application did not recover the new value? "
            f"recovered value: {recovered_value!r} != new value: {new_value!r};\n"
            f"old value: {old_value!r}\napplied delta = {parsed_delta!r}\n"
            f"detected differences: {diff!r}"
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

    pyright exclusions on private ContextStore methods access is intentional;
    the Context can have access to the context store's internal _append_entry method,
    but it should not be exposed to the outside world;
    adding an exclusion rather than making internal classes or making the
    API public looks like a better tradeoff
    """

    _id: ContextId
    _payload: Any
    _user_data: dict[str, Any]
    _parent_contexts: tuple[ParentContextSnapshot, ...]
    _is_completed: bool

    _context_store: ContextStore

    @property
    def id(self) -> ContextId:
        return self._id

    def copy_payload(self) -> AnyPayload:
        """
        Return a deep copy of the context's current payload.
        """
        return copy.deepcopy(self._payload)

    def copy_user_data(self) -> dict[str, AnyUserData]:
        """
        Return a deep copy of the context's current user data.
        """
        return copy.deepcopy(self._user_data)

    def copy_parent_context_snapshots(self) -> ImmutableMap[ContextId, ParentContextSnapshot]:
        """
        Return parent snapshots keyed by parent context ID.

        The snapshots and their contents are deep copies of the state captured when this context was created.
        Mapping iteration order is not part of the API; access snapshots by their parent context ID.
        """
        parent_contexts = copy.deepcopy(self._parent_contexts)
        return ImmutableMap((snapshot.ctx_id, snapshot) for snapshot in parent_contexts)

    @property
    def is_completed(self) -> bool:
        return self._is_completed

    def __init__(
        self,
        _id: ContextId,
        payload: Any,
        user_data: dict[str, AnyUserData],
        parent_contexts: tuple[ParentContextSnapshot, ...],
        context_store: ContextStore,
    ) -> None:
        self._id = _id
        self._payload = payload
        self._user_data = user_data
        self._parent_contexts = parent_contexts
        self._is_completed = False
        self._context_store = context_store

    def append_message[T](self, source: Source[T], payload: T):
        self._assert_mutable()
        delta = deepdiff.Delta(deepdiff.DeepDiff(self._payload, payload))
        payload_delta = cast(bytes, delta.dumps())
        self._context_store._append_entry(  # pyright: ignore[reportPrivateUsage]
            self._id, MessageSent(source=source.id, payload_delta=payload_delta)
        )

        _assert_recovery(self._payload, payload_delta, payload)

        self._payload = copy.deepcopy(payload)

    def set_user_data(self, key: str, value: Any) -> None:
        self._assert_mutable()
        old_value = self._user_data.get(key, None)
        delta = deepdiff.Delta(deepdiff.DeepDiff(old_value, value))
        value_delta = cast(bytes, delta.dumps())
        self._context_store._append_entry(self._id, UserDataChange(key=key, value_delta=value_delta))  # pyright: ignore[reportPrivateUsage]

        _assert_recovery(old_value, value_delta, value)

        self._user_data[key] = copy.deepcopy(value)

    def append_user_note(self, note: str) -> None:
        self._assert_mutable()
        self._context_store._append_entry(self._id, UserNote(note=note))  # pyright: ignore[reportPrivateUsage]

    def complete(self) -> None:
        self._assert_mutable()
        self._context_store._append_entry(self._id, ContextCompleted())  # pyright: ignore[reportPrivateUsage]
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
    a use case where one child context is created to execute validation, and another child context continues
    to return a response to the user). Single-parent children inherit their parent's current payload and user data.

    Multiple parent context can be gathered together to create a child context that represents
    the combined processing of all parent contexts. E.g. consider a use case where multiple
    user requests are batched together to be processed as a single unit, and a child context
    is created to represent the processing of the batch. Later on, the child context can
    be scattered again to return individual responses to each individual user request. Multi-parent children
    keep empty payload/user data and expose creation-time parent snapshots through
    Context.copy_parent_context_snapshots(), keyed by parent context ID. Snapshot iteration order is not part
    of the API. Repeated parent IDs produce one parent-child relationship and one snapshot.

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

    Context lifetime:
    - The store records the last activity of every context: its creation, every log entry appended to it,
      including the creation of a child, and every release of its ownership (also after read-only access). Contexts
      restored by recover_from() are active as of recovery.
    - expire_idle() completes every context idle for longer than a TTL, removes it from the store and its lock, and
      notifies ContextStorePersistence.release_context(). Children keep their parent snapshots, so a parent need not
      outlive its children. An expired context cannot be accessed or used as a parent.
    - Nothing expires contexts unless an expiry TTL is configured (see SubnetBuilder's context_ttl); by default
      contexts are kept for the lifetime of the process.
    """

    __persistence: ContextStorePersistence
    __contexts: dict[ContextId, Context]
    __last_activity: dict[ContextId, datetime.timedelta]
    # guards mutation of __contexts and __last_activity; expiry iterates them while other threads create contexts
    __registry_lock: threading.Lock
    __clock: MonotonicClock
    __locks: ContextStoreLocks

    @classmethod
    def recover_from(
        cls: type[ContextStore], persistence: ContextStorePersistence, *, clock: MonotonicClock = monotonic_now
    ) -> RecoveredContextStore:
        """
        Recovers a ContextStore from the given persistence layer by replaying all log entries.

        `clock` measures context idle time for expire_idle(); tests may inject a fake one.

        pyright exclusions on Context private members is intentional; this is recovery code
        which we want to be able to mutate the context's payload directly

        Raises:
            InternalFrameworkException: if recovered log entries violate framework invariants.
            InternalStateCorruptionException: if log entries violate context history consistency.

        """
        contexts: dict[ContextId, Context] = {}
        last_messages: LastMessages = {}
        completed_contexts: set[ContextId] = set()

        steps: dict[ContextId, StepIdx] = {}

        context_store = ContextStore(token="factory_method")

        for log_entry in persistence.log_entries():
            ctx = log_entry.ctx
            previous_step = steps.get(ctx, None)
            if not (
                (previous_step is None and log_entry.step_idx == StepIdx(0))
                or (previous_step is not None and log_entry.step_idx == previous_step + 1)
            ):
                raise InternalStateCorruptionException(
                    f"log entry step idx out of order for context {ctx}: "
                    f"expected {log_entry.step_idx} to be after {previous_step}"
                )
            steps[ctx] = log_entry.step_idx
            if ctx in completed_contexts:
                raise InternalStateCorruptionException(
                    f"Log entry {log_entry.step_idx} for context {ctx} appears after completion."
                )
            match log_entry.data:
                case ContextCreated():
                    if ctx in contexts:
                        raise InternalStateCorruptionException(f"Context {ctx} already exists during recovery.")
                    context = Context(
                        _id=ctx,
                        payload=None,
                        user_data={},
                        parent_contexts=(),
                        context_store=context_store,
                    )
                    contexts[ctx] = context
                case ContextDataInitialized(
                    payload=payload,
                    user_data=user_data,
                    parent_contexts=parent_contexts,
                ):
                    context = contexts.get(ctx, None)
                    if context is None:
                        raise InternalStateCorruptionException(
                            f"ContextDataInitialized for missing context {ctx} during recovery."
                        )
                    context._payload = copy.deepcopy(payload)  # pyright: ignore[reportPrivateUsage]
                    context._user_data = copy.deepcopy(user_data)  # pyright: ignore[reportPrivateUsage]
                    context._parent_contexts = copy.deepcopy(parent_contexts)  # pyright: ignore[reportPrivateUsage]
                case ChildContextCreated():
                    pass
                case MessageSent(payload_delta=payload_delta) as message:
                    context = contexts.get(ctx, None)
                    if context is None:
                        raise InternalStateCorruptionException(
                            f"MessageSent for missing context {ctx} during recovery."
                        )
                    context._payload += deepdiff.Delta(payload_delta, deserializer=DELTA_DESERIALIZER)  # pyright: ignore[reportPrivateUsage]
                    last_messages[ctx] = message
                case UserNote():
                    # UserNotes do not affect the context state, so we can ignore them during recovery
                    pass
                case UserDataChange(key=key, value_delta=value_delta):
                    context = contexts.get(ctx, None)
                    if context is None:
                        raise InternalStateCorruptionException(
                            f"UserDataChange for missing context {ctx} during recovery."
                        )
                    context._user_data[key] = context._user_data.get(key, None) + deepdiff.Delta(  # pyright: ignore[reportPrivateUsage]
                        value_delta, deserializer=DELTA_DESERIALIZER
                    )
                case ContextCompleted():
                    context = contexts.pop(ctx, None)
                    if context is None:
                        raise InternalStateCorruptionException(
                            f"ContextCompleted for missing context {ctx} during recovery."
                        )
                    context._is_completed = True  # pyright: ignore[reportPrivateUsage]
                    completed_contexts.add(ctx)
                    last_messages.pop(ctx, None)

        # true initialization of the ContextStore
        context_store.__persistence = persistence
        context_store.__contexts = contexts
        context_store.__clock = clock
        context_store.__last_activity = dict.fromkeys(contexts, clock())
        context_store.__registry_lock = threading.Lock()
        context_store.__locks = ThreadContextStoreLocks()
        for context_id in contexts:
            context_store.__locks.register_context(context_id)

        return RecoveredContextStore(context_store, last_messages)

    def __init__(self, token: str = "intialize_using_factory_methods_not_directly") -> None:
        if token != "factory_method":
            raise InternalFrameworkException("ContextStore should be initialized using factory methods, not directly.")

    @contextmanager
    def create_context(self, *, parents: tuple[ContextId, ...] = ()) -> Generator[Context]:
        """
        Creates a new Context with the given parent contexts, and yields it with a context manager
        that ensures mutual exclusion on the new context.

        Parents get locked for the duration of the context creation to ensure that no new log entries
        are appended to them during the creation process

        There is an important distinction between two cases - a single parent child and multiparent child:
        A single parent child context contains a deep copy of the parent context's payload and user data
        Multiparent child context has empty payload and user data.
        In both cases the snapshots of parent contexts' contents are available through
        copy_parent_context_snapshots(), keyed by parent context ID. Duplicate parent IDs are collapsed before
        deciding whether the child has one or multiple parents. Snapshot iteration order is not part of the API.

        Raises:
            InvalidContextIdException: if one of the provided parent context ids does not exist.
            ContextCompletedException: if one of the provided parent contexts is already completed.

        """
        parent_ids = tuple(dict.fromkeys(parents))
        with self._lock_contexts(parent_ids):
            parent_snapshots: list[ParentContextSnapshot] = []
            for parent_id in parent_ids:
                parent_context = self.__contexts.get(parent_id, None)
                if parent_context is None:
                    raise InvalidContextIdException(f"Parent context {parent_id} not found")
                if parent_context.is_completed:
                    raise ContextCompletedException(
                        f"Parent context {parent_id} is completed and cannot create child contexts."
                    )
                parent_snapshots.append(
                    ParentContextSnapshot(
                        ctx_id=parent_context.id,
                        payload=parent_context.copy_payload(),
                        user_data=parent_context.copy_user_data(),
                    )
                )

            parent_contexts = tuple(parent_snapshots)
            if len(parent_contexts) == 1:
                initial_payload = copy.deepcopy(parent_contexts[0].payload)
                initial_user_data = copy.deepcopy(parent_contexts[0].user_data)
            else:
                initial_payload = None
                initial_user_data = {}

            new_context_id = self.__persistence.create_context(parent_ids)
            context = Context(
                _id=new_context_id,
                payload=initial_payload,
                user_data=initial_user_data,
                parent_contexts=copy.deepcopy(parent_contexts),
                context_store=self,
            )
            with self.__registry_lock:
                self.__contexts[new_context_id] = context
            self.__locks.register_context(new_context_id)
            # the persistence appended ChildContextCreated to the parents itself, bypassing _append_entry
            self._touch(new_context_id, *parent_ids)
            if parent_contexts:
                self._append_entry(
                    new_context_id,
                    ContextDataInitialized(
                        payload=copy.deepcopy(initial_payload),
                        user_data=copy.deepcopy(initial_user_data),
                        parent_contexts=copy.deepcopy(parent_contexts),
                    ),
                )

        with self.__locks.lock_context(new_context_id):
            try:
                yield context
            finally:
                self._touch_on_release(context)

    @contextmanager
    def get_context(self, ctx: ContextId) -> Generator[Context]:
        with self.__locks.lock_context(ctx):
            if ctx not in self.__contexts:
                raise InvalidContextIdException(f"Context {ctx} not found")

            context = self.__contexts[ctx]
            try:
                yield context
            finally:
                self._touch_on_release(context)

    def get_user_data[T](
        self,
        ctx_id: ContextId,
        key: str,
        *,
        expected_type: type[T],
    ) -> T:
        """
        Load a user_data value from a context and validate its runtime type.

        Raises:
            InvalidContextIdException: if the context does not exist.
            InternalStateCorruptionException: if the key is missing or value has unexpected type.

        """
        with self.get_context(ctx_id) as context:
            value = context.copy_user_data().get(key)

        if value is None:
            raise InternalStateCorruptionException(f"Missing context user_data for ctx={ctx_id}, key={key!r}.")
        if not isinstance(value, expected_type):
            raise InternalStateCorruptionException(
                f"Unexpected context user_data type for ctx={ctx_id}, key={key!r}: "
                f"expected {expected_type!r}, got {type(value)!r}."
            )
        return value

    def expire_idle(self, ttl: datetime.timedelta) -> int:
        """
        Completes and removes every context idle for longer than `ttl`; returns how many were removed.

        Contexts locked by another thread are in use and are skipped. A failure to expire one context is logged and
        leaves it in the store, so a later call retries it. Context locks are re-entrant, so the caller must not own
        any context: an idle context it owns would be expired while in use.
        """
        now = self.__clock()
        with self.__registry_lock:
            idle = [ctx for ctx, last_activity in self.__last_activity.items() if now - last_activity > ttl]
        expired = 0
        for ctx in idle:
            try:
                if self._expire_if_idle(ctx, ttl):
                    expired += 1
            except Exception:
                logger.exception("Failed to expire idle context %s; it will be retried on the next sweep.", ctx)
        return expired

    def _expire_if_idle(self, ctx: ContextId, ttl: datetime.timedelta) -> bool:
        with self.__locks.try_lock_context(ctx) as acquired:
            if not acquired:
                return False
            with self.__registry_lock:
                last_activity = self.__last_activity[ctx]
            if self.__clock() - last_activity <= ttl:
                return False
            # Complete and release before removing it from the store: if either step fails, the context stays
            # registered and the next sweep retries the remaining steps.
            context = self.__contexts[ctx]
            if not context.is_completed:
                context.complete()
            self.__persistence.release_context(ctx)
            with self.__registry_lock:
                del self.__contexts[ctx]
                del self.__last_activity[ctx]
            self.__locks.unregister_context(ctx)
        return True

    def _append_entry(self, ctx: ContextId, entry: LogEntryData) -> None:
        context = self.__contexts.get(ctx, None)
        if context is None:
            raise InternalFrameworkException(f"Context {ctx} not found in store? It should have been created first.")
        if context.is_completed:
            raise ContextCompletedException(f"Context {ctx} is completed and cannot be mutated.")
        self.__persistence.append_entry(ctx, entry)
        # completion is not activity, so a completed context whose release failed is retried on the next sweep
        if not isinstance(entry, ContextCompleted):
            self._touch(ctx)

    def _touch_on_release(self, context: Context) -> None:
        # Holding a context is activity, so one held for longer than the TTL does not expire right after its release.
        # A completed context is left alone: it is either already removed or awaiting a release retry.
        if not context.is_completed:
            self._touch(context.id)

    def _touch(self, *contexts: ContextId) -> None:
        now = self.__clock()
        with self.__registry_lock:
            for ctx in contexts:
                self.__last_activity[ctx] = now

    @contextmanager
    def _lock_contexts(self, context_ids: Iterable[ContextId]) -> Generator[None]:
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
        if ctx not in self.__data:
            raise InternalFrameworkException(f"Context {ctx} does not exist? It should have been created first.")
        log_entry = LogEntry(
            ctx=ctx,
            step_idx=StepIdx(len(self.__data[ctx])),
            creation_time=datetime.datetime.now(tz=datetime.UTC),
            data=entry,
        )
        self.__data[ctx].append(log_entry)

    @override
    def create_context(self, parents: tuple[ContextId, ...]) -> ContextId:
        """
        Creates a new context that has a parent-child relationship with the given parent contexts.

        Raises:
            InternalFrameworkException: if one of the parent contexts is missing internal state.

        """
        for parent_ctx in parents:
            if parent_ctx not in self.__context_tree:
                raise InternalFrameworkException(
                    f"Context {parent_ctx} does not exist? It should have been created first."
                )
            if parent_ctx not in self.__data:
                raise InternalFrameworkException(
                    f"Context {parent_ctx} data does not exist? It should have been created first."
                )

        child_ctx = ContextId(str(uuid.uuid7()))
        context_creation_time = datetime.datetime.now(tz=datetime.UTC)

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
    def release_context(self, ctx: ContextId) -> None:
        """Drops the context's log entries and its place in the context tree."""
        entries = self.__data.pop(ctx, [])
        self.__context_tree.pop(ctx, None)
        if not entries:
            return
        created = entries[0].data
        if not isinstance(created, ContextCreated):
            raise InternalStateCorruptionException(f"Context {ctx} log does not start with ContextCreated.")
        for parent in created.parents:
            # the parent may have been released before its child
            children = self.__context_tree.get(parent, [])
            if ctx in children:
                children.remove(ctx)

    @override
    def log_entries(self) -> tuple[LogEntry, ...]:
        all_entries: list[LogEntry] = []
        for log_entries in self.__data.values():
            all_entries.extend(log_entries)
        all_entries.sort(key=lambda entry: entry.step_idx)
        return tuple(all_entries)
