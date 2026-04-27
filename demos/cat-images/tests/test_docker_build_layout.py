import tomllib
from pathlib import Path
from typing import Any, cast

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
DEMO_ROOT = Path(__file__).resolve().parents[1]
DOCKER_DIR = DEMO_ROOT / "docker"

DOCKERFILES = (
    DOCKER_DIR / "facilitator.Dockerfile",
    DOCKER_DIR / "miner.Dockerfile",
    DOCKER_DIR / "validator.Dockerfile",
)
BUILD_SCRIPTS = (DEMO_ROOT / "build_miner.sh", DEMO_ROOT / "build_validator.sh")
LOCALNET_ENV_FILES = (DEMO_ROOT / ".env.compose.example", DOCKER_DIR / ".env.validator-compose.example")


def _load_yaml(path: Path) -> dict[str, Any]:
    return cast("dict[str, Any]", yaml.safe_load(path.read_text(encoding="utf-8")))


def _nexus_context(compose_path: Path, service: str) -> Path:
    services = cast("dict[str, Any]", _load_yaml(compose_path)["services"])
    service_def = cast("dict[str, Any]", services[service])
    build = cast("dict[str, Any]", service_def["build"])
    contexts = cast("dict[str, str]", build["additional_contexts"])
    return (compose_path.parent / contexts["nexus-lib"]).resolve()


def test_compose_files_supply_repository_root_as_nexus_context() -> None:
    expected_contexts = {
        DEMO_ROOT / "compose.yaml": ("validator", "miner", "facilitator"),
        DOCKER_DIR / "docker-compose.validator.yaml": ("validator",),
        DOCKER_DIR / "docker-compose.miner.yaml": ("miner",),
    }

    for compose_path, services in expected_contexts.items():
        for service in services:
            assert _nexus_context(compose_path, service) == REPO_ROOT


def test_build_scripts_supply_repository_root_as_nexus_context() -> None:
    for script in BUILD_SCRIPTS:
        text = script.read_text(encoding="utf-8")
        assert '--build-context nexus-lib="$SCRIPT_DIR/../.."' in text


def test_docker_workdirs_match_cat_images_uv_source_path() -> None:
    pyproject = tomllib.loads((DEMO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    source = pyproject["tool"]["uv"]["sources"]["bittensor-nexus-library"]
    assert source["path"] == "../.."

    for dockerfile in DOCKERFILES:
        text = dockerfile.read_text(encoding="utf-8")
        assert "WORKDIR /app/demos/cat-images" in text
        assert 'ENV PATH="/app/demos/cat-images/.venv/bin:$PATH"' in text


def test_dockerfiles_copy_nexus_library_to_uv_source_root() -> None:
    for dockerfile in DOCKERFILES:
        text = dockerfile.read_text(encoding="utf-8")
        assert "COPY --from=nexus-lib pyproject.toml README.md /app/" in text
        assert "COPY --from=nexus-lib src/ /app/src/" in text


def test_localnet_defaults_target_subnet_two() -> None:
    for env_file in LOCALNET_ENV_FILES:
        text = env_file.read_text(encoding="utf-8")
        assert "PYLON_BITTENSOR_NETWORK=http://host.docker.internal:9944" in text
        assert "PYLON_RECENT_OBJECTS_NETUIDS=[2]" in text
        assert "PYLON_ID_VALIDATOR_NETUID=2" in text
        assert "VALIDATOR_NETUID=2" in text

    validator_compose = (DOCKER_DIR / "docker-compose.validator.yaml").read_text(encoding="utf-8")
    assert "PYLON_RECENT_OBJECTS_NETUIDS:-[2]" in validator_compose
    assert "PYLON_ID_VALIDATOR_NETUID:-2" in validator_compose
    assert "VALIDATOR_NETUID:-2" in validator_compose
