import logging
from pathlib import Path

from litestar import Litestar
from litestar.contrib.jinja import JinjaTemplateEngine
from litestar.di import Provide
from litestar.template import TemplateConfig

from cat_images.facilitator.routes.ui import UiController
from cat_images.facilitator.routing import RandomStrategy, ValidatorRouter
from cat_images.facilitator.s3 import S3Client
from cat_images.facilitator.settings import FacilitatorSettings
from cat_images.facilitator.stores import JobStore, ValidatorStore
from cat_images.facilitator.submitter import JobSubmitter
from cat_images.facilitator.worker import JobWorker

_TEMPLATES_DIR = Path(__file__).parent / "templates"


def create_app(settings: FacilitatorSettings | None = None) -> Litestar:
    if settings is None:
        settings = FacilitatorSettings()  # type: ignore[call-arg]

    validator_store = ValidatorStore(validators=settings.validators)
    job_store = JobStore()
    s3_client = S3Client(settings)
    router = ValidatorRouter(strategy=RandomStrategy(), validator_store=validator_store)
    submitter = JobSubmitter(
        max_retries=settings.submit_max_retries,
        timeout=settings.submit_timeout_seconds,
    )
    worker = JobWorker(job_store=job_store, submitter=submitter)

    return Litestar(
        route_handlers=[UiController],
        dependencies={
            "settings": Provide(lambda: settings),
            "validator_store": Provide(lambda: validator_store),
            "job_store": Provide(lambda: job_store),
            "s3_client": Provide(lambda: s3_client),
            "validator_router": Provide(lambda: router),
            "worker": Provide(lambda: worker),
        },
        template_config=TemplateConfig(
            directory=_TEMPLATES_DIR,
            engine=JinjaTemplateEngine,
        ),
    )
