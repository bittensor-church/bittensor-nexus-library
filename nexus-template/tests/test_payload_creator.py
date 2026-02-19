# pyright: basic

import http.client
import queue
import urllib.parse
import uuid

from utils import CollectorActor, Jobs, empty_context_store, wait_until

from nexus.actors.payload_creator import (
    S3PresignedUrlCreator,
    WithS3PresignedUrl,
)
from nexus.core.dsl.flow import Flow
from nexus.core.dsl.nodes import Source
from nexus.core.dsl.piping import Piping
from nexus.core.runtime.event_bus import EventBus
from nexus.core.runtime.events import PipeToBus, SendEvent


def test_s3_presigned_url_creator_actor_adds_presigned_put_url_and_wraps_attempt(
    default_s3_storage_client,
    default_test_s3_bucket: str,
) -> None:
    admin_client = default_s3_storage_client

    context_store = empty_context_store()
    pipe_to_bus: PipeToBus = queue.Queue()
    creator = S3PresignedUrlCreator[str](
        "presigner",
        bucket=default_test_s3_bucket,
    )
    creator_actor = creator.build_actor(pipe_to_bus=pipe_to_bus, context_store=context_store)
    collector = CollectorActor[WithS3PresignedUrl[str]](
        pipe_to_bus=pipe_to_bus,
        context_store=context_store,
    )

    upstream_source = Source[str]("attempt-source")
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

    jobs = Jobs(event_bus.run_loop(), creator_actor.run_loop(), collector.run_loop())
    try:
        pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=upstream_source, payload="input-payload"))
        wait_until(lambda: len(collector.received_events) == 1)

        received = collector.received_events[0]
        assert received.ctx_id == ctx_id
        assert received.target == collector.sink

        request_info: WithS3PresignedUrl[str] = received.payload
        assert request_info.input == "input-payload"

        presigned_url = request_info.s3_presigned_url
        assert "X-Amz-Signature=" in presigned_url or "Signature=" in presigned_url
        assert f"/{default_test_s3_bucket}/" in presigned_url

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

        uploaded = admin_client.get_object(Bucket=default_test_s3_bucket, Key=s3_key)
        assert uploaded["Body"].read() == upload_data
    finally:
        event_bus.request_stop()
        jobs.join()
