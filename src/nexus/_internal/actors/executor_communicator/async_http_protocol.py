from __future__ import annotations

from typing import NewType

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, JsonValue

RequestId = NewType("RequestId", str)


class AsyncHttpNeuronRequestEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: RequestId
    callback_url: AnyHttpUrl
    input: dict[str, JsonValue]


class AsyncHttpNeuronResponseEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: RequestId
    output: dict[str, JsonValue] | None = None
    error: str | None = None
