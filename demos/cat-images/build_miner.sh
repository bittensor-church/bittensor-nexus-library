#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <image:tag> [extra docker build args...]" >&2
  exit 1
fi

IMAGE="$1"
shift

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

docker build \
  -f "$SCRIPT_DIR/docker/miner.Dockerfile" \
  --build-context nexus-lib="$SCRIPT_DIR/../nexus-template" \
  -t "$IMAGE" \
  "$@" \
  "$SCRIPT_DIR"