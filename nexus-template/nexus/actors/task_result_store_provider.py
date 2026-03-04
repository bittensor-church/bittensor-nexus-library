from abc import ABC, abstractmethod
from typing import Any, cast, override

from nexus.core.runtime.task_result_store import InMemoryTaskResultStore, TaskResultStore

DEFAULT_TASK_RESULT_STORE: TaskResultStore[Any, Any] = InMemoryTaskResultStore()


class TaskResultStoreProvider[ExecutorPayload, Output](ABC):
    @abstractmethod
    def get_task_result_store(self) -> TaskResultStore[ExecutorPayload, Output]: ...


class DefaultTaskResultStoreProvider[ExecutorPayload, Output](TaskResultStoreProvider[ExecutorPayload, Output]):
    @override
    def get_task_result_store(self) -> TaskResultStore[ExecutorPayload, Output]:
        return cast(TaskResultStore[ExecutorPayload, Output], DEFAULT_TASK_RESULT_STORE)
