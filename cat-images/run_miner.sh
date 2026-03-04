#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DOCKER_DIR="${SCRIPT_DIR}/docker"

DEFAULT_COMPOSE_FILE_YAML="${DOCKER_DIR}/docker-compose.miner.yaml"
DEFAULT_COMPOSE_FILE_YML="${DOCKER_DIR}/docker-compose.miner.yml"

if [[ -n "${MINER_COMPOSE_FILE:-}" ]]; then
  COMPOSE_FILE="${MINER_COMPOSE_FILE}"
elif [[ -f "${DEFAULT_COMPOSE_FILE_YAML}" ]]; then
  COMPOSE_FILE="${DEFAULT_COMPOSE_FILE_YAML}"
elif [[ -f "${DEFAULT_COMPOSE_FILE_YML}" ]]; then
  COMPOSE_FILE="${DEFAULT_COMPOSE_FILE_YML}"
else
  echo "No miner compose file found." >&2
  echo "Expected one of:" >&2
  echo "  ${DEFAULT_COMPOSE_FILE_YAML}" >&2
  echo "  ${DEFAULT_COMPOSE_FILE_YML}" >&2
  exit 1
fi

ENV_FILE="${MINER_ENV_FILE:-${DOCKER_DIR}/.env.miner}"
ENV_TEMPLATE="${MINER_ENV_TEMPLATE:-${DOCKER_DIR}/.env.miner-compose.example}"

usage() {
  cat <<EOF
Run miner docker compose stack.

Usage:
  ./run_miner.sh [command] [extra args...]

Commands:
  up       Start miner in detached mode (default): docker compose up --build -d
  down     Stop and remove miner
  logs     Follow miner logs
  ps       Show service status
  config   Render resolved compose config
  restart  Restart miner
  pull     Pull images
  help     Show this help

Script-level environment variables:
  MINER_COMPOSE_FILE     Compose file path override
  MINER_ENV_FILE         Env file path override (default: ${DOCKER_DIR}/.env.miner)
  MINER_ENV_TEMPLATE     Template path shown when env file is missing

Compose environment variables (typically defined in ${DOCKER_DIR}/.env.miner):
  Image/network/container:
    MINER_IMAGE                  Miner image tag (default: cat-miner:latest)
    MINER_CONTAINER_NAME         Container name (default: cat-miner)
    MINER_DOCKER_NETWORK         Existing Docker network to join
                                 (default: cat-images-validator-net)
    MINER_CONTAINER_IP           Static container IP on validator network
                                 (default: 172.30.0.30)
    MINER_WALLET_DIR             Host wallet path mounted to /root/.bittensor
                                 (default: \$HOME/.bittensor)

  Port publishing:
    MINER_HOST_BIND_IP           Host bind IP (default: 127.0.0.1)
    MINER_HOST_PORT              Host published port (default: 9090)
    MINER_PORT                   Miner listen port in container (default: 9090)

  Miner application environment variables (from cat_images/miner.py):
  Required:
    MINER_OPENROUTER_API_KEY     OpenRouter API key.

  Optional:
    MINER_OPENROUTER_URL         OpenRouter endpoint.
                                 Default: https://openrouter.ai/api/v1/chat/completions
    MINER_OPENROUTER_MODEL       OpenRouter model.
                                 Default: google/gemini-2.5-flash-image
    MINER_PROMPT                 Prompt sent to model (default: built-in prompt)
    MINER_PATH                   Miner HTTP path.
                                 Default: /process
    MINER_WALLET_NAME            Wallet name (default: default)
    MINER_HOTKEY_NAME            Wallet hotkey name (default: default)

  Axon updater options:
    MINER_UPDATE_AXON            true/false (default: false)
    MINER_SUBTENSOR_NETWORK      Subtensor network (default: finney)
    MINER_NETUID                 Required when MINER_UPDATE_AXON=true
    MINER_EXTERNAL_IP            Advertised IP (auto-detected if unset)
    MINER_EXTERNAL_PORT          Advertised port (defaults to MINER_PORT if unset)
    MINER_SERVE_INTERVAL         Axon update interval (default: 60 seconds)

Example:
  cp docker/.env.miner-compose.example docker/.env.miner
  ./run_miner.sh up
  ./run_miner.sh logs
  ./run_miner.sh down
EOF
}

COMMAND="${1:-up}"
if [[ $# -gt 0 ]]; then
  shift
fi

if [[ "${COMMAND}" == "help" || "${COMMAND}" == "--help" || "${COMMAND}" == "-h" ]]; then
  usage
  exit 0
fi

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing env file: ${ENV_FILE}" >&2
  if [[ -f "${ENV_TEMPLATE}" ]]; then
    echo "Create it with: cp ${ENV_TEMPLATE} ${ENV_FILE}" >&2
  fi
  exit 1
fi

compose_cmd=(docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}")

case "${COMMAND}" in
  up)
    exec "${compose_cmd[@]}" up --build -d "$@"
    ;;
  down)
    exec "${compose_cmd[@]}" down "$@"
    ;;
  logs)
    exec "${compose_cmd[@]}" logs -f "$@"
    ;;
  ps)
    exec "${compose_cmd[@]}" ps "$@"
    ;;
  config)
    exec "${compose_cmd[@]}" config "$@"
    ;;
  restart)
    exec "${compose_cmd[@]}" restart "$@"
    ;;
  pull)
    exec "${compose_cmd[@]}" pull "$@"
    ;;
  *)
    echo "Unknown command: ${COMMAND}" >&2
    usage >&2
    exit 1
    ;;
esac
