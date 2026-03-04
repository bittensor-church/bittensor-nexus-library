import asyncio
import logging
from collections.abc import AsyncGenerator
from typing import Any

import httpx
from litestar import Controller, Request, get, post
from litestar.datastructures import UploadFile
from litestar.exceptions import NotFoundException
from litestar.response import Response, ServerSentEvent, Template

from cat_images.facilitator.models import CatificationRequest, Job
from cat_images.subnet import S3Url, SingleCatImageInput
from cat_images.facilitator.types import JobId, JobLiveness, ValidatorHotkey
from cat_images.facilitator.routing import ValidatorRouter
from cat_images.facilitator.s3 import S3Client
from cat_images.facilitator.stores import JobStore, ValidatorStore
from cat_images.facilitator.worker import JobWorker

log = logging.getLogger("facilitator.ui")

_STATUS_TEMPLATE = "fragments/_status_entry.html"


def _sse_event(event: str, data: str) -> dict[str, str]:
    return {"event": event, "data": data}


def _render_terminal(job: Job, jinja_env: Any) -> dict[str, str]:
    if job.liveness == JobLiveness.SUCCESS:
        html = jinja_env.get_template("fragments/_result.html").render(job=job)
        return _sse_event("result", html)
    html = jinja_env.get_template("fragments/_error.html").render(job=job)
    return _sse_event("error", html)


_POLL_INTERVAL = 0.3  # seconds


async def _generate_events(
    job_id: str,
    job_store: JobStore,
    request: Request,
) -> AsyncGenerator[dict[str, str], None]:
    jinja_env = request.app.template_engine.engine  # type: ignore[union-attr]
    seen = 0

    while True:
        job = job_store.get(job_id)  # type: ignore[arg-type]
        if job is None:
            return

        for update in job.status_updates[seen:]:
            html = jinja_env.get_template(_STATUS_TEMPLATE).render(update=update, job=job)
            yield _sse_event("status", html)
            seen += 1

        if job.is_terminal:
            yield _render_terminal(job, jinja_env)
            yield _sse_event("close", "")
            return

        await asyncio.sleep(_POLL_INTERVAL)


class UiController(Controller):
    path = "/"

    @get("/")
    async def landing(self, request: Request) -> Template:
        return Template(template_name="landing.html")

    @post("/catify")
    async def catify(
        self,
        request: Request,
        s3_client: S3Client,
        job_store: JobStore,
        validator_router: ValidatorRouter,
        worker: JobWorker,
    ) -> Template:
        form = await request.form()
        file = form.get("image")

        if not isinstance(file, UploadFile):
            return Template(
                template_name="fragments/_error.html",
                context={"job": None, "error_message": "No image file provided"},
            )

        file_bytes = await file.read()
        content_type = file.content_type or "image/png"

        # Upload to S3
        image_key = s3_client.upload_image(file_bytes, content_type)
        image_url = s3_client.presign(image_key)
        job_spec = SingleCatImageInput(image_s3_url=S3Url(image_url))

        # Route: pick a validator for this request
        cat_request = CatificationRequest(job_spec=job_spec)
        validator = validator_router.select(cat_request)
        if validator is None:
            return Template(
                template_name="fragments/_error.html",
                context={"job": None, "error_message": "No validators available"},
            )

        # Persist the job and dispatch to background worker
        job = job_store.create(job_spec, validator.hotkey, image_key)
        worker.dispatch(job.id, job_spec, validator)

        return Template(
            template_name="fragments/_progress.html",
            context={"job": job},
        )

    @get("/jobs/{job_id:str}")
    async def job_detail(self, job_store: JobStore, job_id: str) -> Template:
        job = job_store.get(JobId(job_id))
        if job is None:
            raise NotFoundException(detail=f"Job {job_id} not found")
        return Template(template_name="job_detail.html", context={"job": job})

    @get("/jobs/{job_id:str}/stream")
    async def job_stream(self, request: Request, job_store: JobStore, job_id: str) -> ServerSentEvent:
        return ServerSentEvent(
            _generate_events(job_id, job_store, request),
            event_type="message",
        )

    @get("/jobs")
    async def job_list(self, job_store: JobStore) -> Template:
        return Template(
            template_name="job_list.html",
            context={"jobs": job_store.list_all()},
        )

    @get("/images/{job_id:str}/{kind:str}")
    async def proxy_image(self, job_store: JobStore, s3_client: S3Client, job_id: str, kind: str) -> Response:
        """Proxy S3 images to avoid CORS/ORB issues."""
        job = job_store.get(job_id)  # type: ignore[arg-type]
        if job is None:
            raise NotFoundException(detail=f"Job {job_id} not found")

        if kind == "original":
            # We have the S3 key — download directly
            data, content_type = s3_client.download(job.image_key)
            return Response(content=data, media_type=content_type)
        elif kind == "result":
            url = str(job.result.result_image_url) if job.result else None
            if not url:
                raise NotFoundException(detail="Result image not available")
            # Result URL comes from the validator — fetch via HTTP
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(url)
                resp.raise_for_status()
            content_type = resp.headers.get("content-type", "image/png")
            return Response(content=resp.content, media_type=content_type)
        else:
            raise NotFoundException(detail=f"Unknown image kind: {kind}")

    @get("/validators")
    async def validators_page(self, validator_store: ValidatorStore) -> Template:
        return Template(
            template_name="validators.html",
            context={"validators": validator_store.list_all()},
        )

    @post("/validators/{hotkey:str}/delete")
    async def delete_validator(self, validator_store: ValidatorStore, hotkey: str) -> str:
        validator_store.delete(ValidatorHotkey(hotkey))
        return ""

    @post("/validators/{hotkey:str}/toggle")
    async def toggle_validator(self, validator_store: ValidatorStore, hotkey: str) -> Template:
        v = validator_store.toggle(ValidatorHotkey(hotkey))
        if v is None:
            raise NotFoundException(detail=f"Validator {hotkey} not found")
        return Template(
            template_name="fragments/_validator_row.html",
            context={"v": v},
        )
