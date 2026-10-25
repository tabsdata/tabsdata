#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import importlib
import pkgutil

import pytest

from tabsdatak.server._tdrunner.function import commands
from tabsdatak.server._tdrunner.function._commons.run.version import version

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

SECOND_GENERATION = {"get_bundle"}


def versioned() -> list[str]:
    names = []
    for module in pkgutil.iter_modules(commands.__path__):
        command = importlib.import_module(f"{commands.__name__}.{module.name}")
        if hasattr(command, "VERSION"):
            names.append(module.name)
    return sorted(names)


@pytest.mark.parametrize("name", versioned())
def test_command_declares_its_version_and_generation(name: str):
    command = importlib.import_module(f"{commands.__name__}.{name}")
    assert command.VERSION == version()
    assert command.GENERATION == (2 if name in SECOND_GENERATION else 1)
