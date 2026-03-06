from abc import ABC, abstractmethod
from typing import Any, cast, override

from nexus.core.runtime.task_result_store import InMemoryTaskResultStore, TaskResultStore

DEFAULT_TASK_RESULT_STORE: TaskResultStore[Any, Any, Any] = InMemoryTaskResultStore()


class TaskResultStoreProvider[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](ABC):
    @abstractmethod
    def get_task_result_store(self) -> TaskResultStore[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]: ...


class DefaultTaskResultStoreProvider[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
    TaskResultStoreProvider[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]
):
    @override
    def get_task_result_store(self) -> TaskResultStore[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]:
        return cast(TaskResultStore[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput], DEFAULT_TASK_RESULT_STORE)


DEFAULT_TASK_RESULT_STORE_PROVIDER: TaskResultStoreProvider[Any, Any, Any] = DefaultTaskResultStoreProvider()
