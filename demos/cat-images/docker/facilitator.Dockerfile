# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_DEV=1 \
    UV_FROZEN=1

WORKDIR /app/demos/cat-images

# Local path dependency at ../.. from the cat-images project.
COPY --from=nexus-lib pyproject.toml README.md /app/
COPY --from=nexus-lib src/ /app/src/

# Deps only — source changes don't bust this layer
# adding --no-install-project otherwise the project source ends up in .venv
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --group facilitator --no-install-project

# Project source + install
COPY cat_images/ cat_images/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --group facilitator

FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

COPY --from=builder /app /app

ENV PATH="/app/demos/cat-images/.venv/bin:$PATH"
WORKDIR /app/demos/cat-images
EXPOSE 8080
ENTRYPOINT ["python", "-m", "cat_images.facilitator"]
