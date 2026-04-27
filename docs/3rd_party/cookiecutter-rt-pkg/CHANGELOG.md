# cookiecutter-rt-pkg Changelog

Main purpose of this file is to provide a changelog for the template itself.
It is not intended to be used as a changelog for the generated project.

This changelog will document any know **BREAKING** changes between versions of the template.
Please review this new entries carefully after applying `cruft update` before committing the changes.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

Currently, `cookiecutter-rt-pkg` has no explicit versioning amd we purely rely on `cruft` diff.

## v0.1.0 (2026-04-17)

### Feat

- remove unused directive
- replace PDM to UV

### Fix

- make readable happy
- make `django_versions` cookiecutter var empty by default
- exclude redundant import in `conftest.py`
- add release type check to `publish-docker` job and stop if type is `prerelease` or `unknown`
- include `prerelease` label on release notes; refactor release type names
- fail-fast on invalid release tag
- upgrade gh-action-sigstore-python to resolve deployment to PYPI issues
- resolve nox error with uv support
- dockerfile changes
- tool.setuptools_scm for docker build

### Refactor

- skip unmatching steps instead of marking job as failed
- replace outer version management from towncrier to commitizen
