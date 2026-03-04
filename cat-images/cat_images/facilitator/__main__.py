import logging
import sys

import uvicorn
from pydantic import ValidationError

from cat_images.facilitator.app import create_app
from cat_images.facilitator.settings import FacilitatorSettings

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s", datefmt="%H:%M:%S", level=logging.INFO
)
log = logging.getLogger("facilitator")


def main() -> None:
    try:
        settings = FacilitatorSettings()  # type: ignore[call-arg]
    except ValidationError as e:
        fields = ", ".join(str(err["loc"][-1]) for err in e.errors() if err.get("loc"))
        log.error(f"Configuration error: missing or invalid fields: {fields}")
        sys.exit(1)

    log.info("Facilitator config:\n%s", settings.model_dump_json(indent=2))
    app = create_app(settings)
    uvicorn.run(app, host=settings.host, port=settings.port)


if __name__ == "__main__":
    main()
