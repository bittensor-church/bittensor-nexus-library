import uuid
from typing import NewType

NexusTaskName = NewType("NexusTaskName", str)

TaskResultId = NewType("TaskResultId", uuid.UUID)
