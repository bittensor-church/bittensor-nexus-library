from importlib import import_module

import pytest


@pytest.mark.parametrize(
    "module_name",
    [
        "nexus.actors",
        "nexus.core",
        "nexus.logging_utils",
        "nexus.nexus_validator",
        "nexus.utils",
    ],
)
def test_public_interface_is_only_available_under_v1(module_name: str) -> None:
    with pytest.raises(ModuleNotFoundError):
        import_module(module_name)


@pytest.mark.parametrize(
    "module_name",
    [
        "nexus.v1",
        "nexus.v1.actors",
        "nexus.v1.actors.chain_beat",
        "nexus.v1.actors.executor_communicator",
        "nexus.v1.core",
        "nexus.v1.core.dsl",
        "nexus.v1.core.runtime",
        "nexus.v1.utils",
    ],
)
def test_public_v1_submodules_are_importable(module_name: str) -> None:
    import_module(module_name)
