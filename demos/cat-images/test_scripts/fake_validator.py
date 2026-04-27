"""
Fake validator for testing the facilitator UI.

Accepts raw UserImageInput, sleeps to simulate work,
and returns a `ValidatorResult` payload with `result_image_url` and `image_hash`.

Usage:
    uv run test_scripts/fake_validator.py              # default port 9999
    uv run test_scripts/fake_validator.py --port 9999
"""

import argparse
import logging
import time

import uvicorn
from litestar import Litestar, post

from cat_images.subnet_models import ImageHash, S3Url, UserImageInput, ValidatorResult

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("fake-validator")


@post("/submit", sync_to_thread=True)
def accept_job(data: UserImageInput) -> ValidatorResult:
    log.info(f"Job received — image_s3_url={data.image_s3_url}")
    time.sleep(5)
    log.info("Done processing")
    return ValidatorResult(
        result_image_url=S3Url("https://http.cat/200.jpg"),
        image_hash=ImageHash("fake-hash"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Fake validator for facilitator testing")
    parser.add_argument("--port", type=int, default=9999)
    args = parser.parse_args()

    app = Litestar(route_handlers=[accept_job])
    log.info(f"Fake validator listening on port {args.port}")
    uvicorn.run(app, host="0.0.0.0", port=args.port)


if __name__ == "__main__":
    main()
