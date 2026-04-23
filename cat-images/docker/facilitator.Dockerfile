# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_DEV=1 \
    UV_FROZEN=1

WORKDIR /app

# Local path dependency — only pyproject + package dir, skip .git/.venv/tests
COPY --from=nexus-library pyproject.toml /bittensor-nexus-library/pyproject.toml
COPY --from=nexus-library nexus/ /bittensor-nexus-library/nexus/

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

# nexus-library is editable (.pth -> /bittensor-nexus-library) because uv re-editables it on the
# second sync and there's no per-package override. Must ship the source.
COPY --from=builder /bittensor-nexus-library /bittensor-nexus-library
COPY --from=builder /app /app

ENV PATH="/app/.venv/bin:$PATH"
WORKDIR /app
EXPOSE 8080
ENTRYPOINT ["python", "-m", "cat_images.facilitator"]
