from __future__ import annotations

import os
from pathlib import Path

import nox

CI = os.environ.get("CI") is not None

ROOT = Path(__file__).resolve().parent
UV_PROJECT_ENV = {"UV_PROJECT_ENVIRONMENT": str(ROOT / ".venv")}


nox.options.default_venv_backend = "uv"
nox.options.stop_on_first_error = True
nox.options.reuse_existing_virtualenvs = not CI


def install(session: nox.Session, *groups: str) -> None:
    session.chdir(ROOT)
    args = ["sync", "--no-default-groups"]
    for group in groups:
        args.extend(["--group", group])
    session.run("uv", *args, external=True, env=UV_PROJECT_ENV)


@nox.session(name="format", tags=["check"])
def format_(session: nox.Session) -> None:
    """Apply the project formatter and safe lint fixes."""
    install(session, "lint")
    session.run("uv", "run", "ruff", "check", "--fix", ".", external=True, env=UV_PROJECT_ENV)
    session.run("uv", "run", "ruff", "format", ".", external=True, env=UV_PROJECT_ENV)


@nox.session(tags=["check"])
def lint(session: nox.Session) -> None:
    """Run the library QA commands in readonly mode."""
    install(session, "lint", "test")
    session.run("uv", "run", "ruff", "check", ".", external=True, env=UV_PROJECT_ENV)
    session.run("uv", "run", "ruff", "format", "--check", ".", external=True, env=UV_PROJECT_ENV)
    session.run("uv", "run", "basedpyright", external=True, env=UV_PROJECT_ENV)


@nox.session(python="3.14", tags=["check"])
def test(session: nox.Session) -> None:
    """Run the library test suite."""
    install(session, "test")
    session.run(
        "uv",
        "run",
        "pytest",
        "-q",
        "--tb=line",
        "-r",
        "f",
        *session.posargs,
        external=True,
        env=UV_PROJECT_ENV,
    )
