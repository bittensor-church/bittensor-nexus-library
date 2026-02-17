# pyright: basic

import http.client
import queue
import urllib.parse
import uuid
from typing import Any

import boto3
from botocore.config import Config
from moto.server import ThreadedMotoServer

from nexus.actors.payload_creator import (
    ExecutionRequestInfo,
    S3PresignedUrlCreator,
    WithS3PresignedUrl,
)
from nexus.actors.retry_strategy import Attempt, AttemptNumber
from nexus.actors.s3_config import S3Config
from nexus.core.dsl.flow import Flow
from nexus.core.dsl.nodes import Sink, Source
from nexus.core.dsl.piping import Piping
from nexus.core.runtime.actor import Actor, EventHandler
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.event_bus import EventBus
from nexus.core.runtime.events import PipeToBus, ReceiveEvent, SendEvent
from utils import Jobs, empty_context_store, wait_until


class CollectorActor[T](Actor):
    def __init__(
        self,
        *,
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
        name: str = "created-payload-collector",
    ) -> None:
        super().__init__(name=name, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.sink = Sink[ExecutionRequestInfo[T, WithS3PresignedUrl[T]]](f"{name}-sink")
        self.received_events: list[ReceiveEvent[ExecutionRequestInfo[T, WithS3PresignedUrl[T]]]] = []

    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {
            self.sink: self._handle
        }


    def _handle(
        self,
        _: Context,
        event: ReceiveEvent[ExecutionRequestInfo[T, WithS3PresignedUrl[T]]],
    ) -> tuple[SendEvent[Any], ...]:
        self.received_events.append(event)
        return ()


def _s3_config(*, endpoint_url: str | None = None) -> S3Config:
    return S3Config(
        bucket_name="uploads",
        aws_access_key_id="test-access-key",
        aws_secret_access_key="test-secret-key",
        region_name="us-east-1",
        endpoint_url=endpoint_url,
        s3_addressing_style="path",
    )


def test_s3_presigned_url_creator_actor_adds_presigned_put_url_and_wraps_attempt() -> None:
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    server.start()
    try:
        host, port = server.get_host_and_port()
        endpoint_url = f"http://{host}:{port}"

        admin_client = boto3.client(
            "s3",
            aws_access_key_id="test-access-key",
            aws_secret_access_key="test-secret-key",
            region_name="us-east-1",
            endpoint_url=endpoint_url,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )
        admin_client.create_bucket(Bucket="uploads")

        context_store = empty_context_store()
        pipe_to_bus: PipeToBus = queue.Queue()
        creator = S3PresignedUrlCreator[str](
            "presigner",
            s3_config=_s3_config(endpoint_url=endpoint_url),
            presigned_url_expiration_seconds=321,
        )
        creator_actor = creator.build_actor(pipe_to_bus=pipe_to_bus, context_store=context_store)
        collector = CollectorActor[str](pipe_to_bus=pipe_to_bus, context_store=context_store)

        upstream_source = Source[Attempt[str]]("attempt-source")
        piping = Piping()
        piping.add_flow(Flow.from_connectable(upstream_source).then(creator.input))
        piping.add_flow(Flow.from_connectable(creator.created_payload).then(collector.sink))

        event_bus = EventBus(
            connections=piping.pipes,
            input_pipe=pipe_to_bus,
            actors=[creator_actor, collector],
            context_store=context_store,
        )

        with context_store.create_context() as created_ctx:
            ctx_id = created_ctx.id

        attempt = Attempt(original_input="payload-to-upload", attempt_number=AttemptNumber(1))

        jobs = Jobs(event_bus.run_loop(), creator_actor.run_loop(), collector.run_loop())
        try:
            pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=upstream_source, payload=attempt))
            wait_until(lambda: len(collector.received_events) == 1)

            received = collector.received_events[0]
            assert received.ctx_id == ctx_id
            assert received.target == collector.sink

            request_info = received.payload
            assert request_info.attempt == attempt
            assert request_info.payload_for_executor.original_input == "payload-to-upload"

            presigned_url = request_info.payload_for_executor.s3_presigned_url
            assert "X-Amz-Signature=" in presigned_url
            assert "/uploads/" in presigned_url

            with context_store.get_context(ctx_id) as context:
                s3_key = context.user_data[creator.id]
                assert uuid.UUID(s3_key).version == 7
                assert s3_key in presigned_url

            upload_data = b"payload-from-third-party"
            parsed_url = urllib.parse.urlsplit(presigned_url)
            path_with_query = parsed_url.path
            if parsed_url.query:
                path_with_query = f"{path_with_query}?{parsed_url.query}"

            assert parsed_url.hostname is not None
            conn = http.client.HTTPConnection(parsed_url.hostname, parsed_url.port)
            try:
                conn.request(
                    "PUT",
                    path_with_query,
                    body=upload_data,
                    headers={"Content-Length": str(len(upload_data))},
                )
                response = conn.getresponse()
                assert response.status == 200
                response.read()
            finally:
                conn.close()

            uploaded = admin_client.get_object(Bucket="uploads", Key=s3_key)
            assert uploaded["Body"].read() == upload_data
        finally:
            event_bus.request_stop()
            jobs.join()
    finally:
        server.stop()
