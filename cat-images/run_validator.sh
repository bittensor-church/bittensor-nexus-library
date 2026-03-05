#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DOCKER_DIR="${SCRIPT_DIR}/docker"

# Prefer .yaml if present, fallback to existing .yml file.
DEFAULT_COMPOSE_FILE_YAML="${DOCKER_DIR}/docker-compose.validator.yaml"
DEFAULT_COMPOSE_FILE_YML="${DOCKER_DIR}/docker-compose.validator.yml"

if [[ -n "${VALIDATOR_COMPOSE_FILE:-}" ]]; then
  COMPOSE_FILE="${VALIDATOR_COMPOSE_FILE}"
elif [[ -f "${DEFAULT_COMPOSE_FILE_YAML}" ]]; then
  COMPOSE_FILE="${DEFAULT_COMPOSE_FILE_YAML}"
elif [[ -f "${DEFAULT_COMPOSE_FILE_YML}" ]]; then
  COMPOSE_FILE="${DEFAULT_COMPOSE_FILE_YML}"
else
  echo "No validator compose file found." >&2
  echo "Expected one of:" >&2
  echo "  ${DEFAULT_COMPOSE_FILE_YAML}" >&2
  echo "  ${DEFAULT_COMPOSE_FILE_YML}" >&2
  exit 1
fi

ENV_FILE="${VALIDATOR_ENV_FILE:-${DOCKER_DIR}/.env.validator}"
ENV_TEMPLATE="${VALIDATOR_ENV_TEMPLATE:-${DOCKER_DIR}/.env.validator-compose.example}"

usage() {
  cat <<EOF
Run validator+pylon docker compose stack.

Usage:
  ./run_validator.sh [command] [extra args...]

Commands:
  up       Start stack in detached mode (default): docker compose up --build -d
  down     Stop and remove stack
  logs     Follow logs (optionally pass service names)
  ps       Show service status
  config   Render resolved compose config
  restart  Restart services
  pull     Pull images
  help     Show this help

Script-level environment variables:
  VALIDATOR_COMPOSE_FILE  Compose file path override
  VALIDATOR_ENV_FILE      Env file path override (default: ${DOCKER_DIR}/.env.validator)
  VALIDATOR_ENV_TEMPLATE  Template path shown when env file is missing

Compose environment variables (typically defined in ${DOCKER_DIR}/.env.validator):
  Required:
    VALIDATOR_OPENROUTER_API_KEY   OpenRouter API key used by validator.
    AWS_ACCESS_KEY_ID              AWS access key for boto/S3.
    AWS_SECRET_ACCESS_KEY          AWS secret key for boto/S3.

  Optional image/network:
    VALIDATOR_IMAGE                Validator image tag (default: cat-validator:compose)
    VALIDATOR_DOCKER_NETWORK       Bridge network name (default: cat-images-validator-net)
    VALIDATOR_DOCKER_SUBNET        Subnet CIDR (default: 172.30.0.0/24)
    VALIDATOR_CONTAINER_IP         Validator container IP (default: 172.30.0.10)
    PYLON_CONTAINER_IP             Pylon container IP (default: 172.30.0.20)
    PYLON_HOST_PORT                Host-local pylon debug port (default: 18000)

  Optional validator runtime:
    VALIDATOR_NETUID               Subnet netuid (default: 1)
    VALIDATOR_REST_ENTRY_POINT_PORT Validator ingress port (default: 8081)
    VALIDATOR_MINER_CALLBACK_PORT  Validator callback port (default: 9091)
    VALIDATOR_OPENROUTER_URL       OpenRouter endpoint
    VALIDATOR_OPENROUTER_MODEL     OpenRouter model
    VALIDATOR_VALIDATION_PROMPT    Prompt used for validator-side image scoring
    VALIDATOR_VALIDATION_OPENROUTER_TIMEOUT_SECONDS
                                  Timeout for validator OpenRouter call (default: 120.0)
    VALIDATOR_VALIDATION_OPENROUTER_TEMPERATURE
                                  Temperature for validator OpenRouter call (default: 0.0)
    VALIDATOR_S3_BUCKET            Bucket used for generated presigned upload URLs
                                   (default: my-cat-images-bucket)
    VALIDATOR_PYLON_SERVICE_ADDRESS Pylon service URL (default: http://pylon:8000)
    VALIDATOR_PYLON_OPEN_ACCESS_TOKEN Pylon access token used by validator client

  Optional pylon runtime:
    PYLON_BITTENSOR_NETWORK        Bittensor network (default: finney)
    PYLON_RECENT_OBJECTS_NETUIDS   JSON list of cached netuids for open-access routing
                                   (example: [278])
    PYLON_OPEN_ACCESS_TOKEN        Pylon open-access token

  Optional AWS:
    AWS_SESSION_TOKEN              Temporary session token, if applicable
    AWS_DEFAULT_REGION             AWS region (default: us-east-1)

Examples:
  ./run_validator.sh
  ./run_validator.sh logs validator
  ./run_validator.sh down
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
