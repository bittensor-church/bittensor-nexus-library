from abc import ABC, abstractmethod
from typing import Any, cast, override

from nexus.core.runtime.task_result_store import InMemoryTaskResultStore, TaskResultStore

DEFAULT_TASK_RESULT_STORE: TaskResultStore[Any] = InMemoryTaskResultStore()


class TaskResultStoreProvider[Result](ABC):
    @abstractmethod
    def get_task_result_store(self) -> TaskResultStore[Result]: ...


class DefaultTaskResultStoreProvider[Result](TaskResultStoreProvider[Result]):
    @override
    def get_task_result_store(self) -> TaskResultStore[Result]:
        return cast(TaskResultStore[Result], DEFAULT_TASK_RESULT_STORE)
