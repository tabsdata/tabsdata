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

SECOND_GENERATION = {
    "lifecycle.begin_execution",
    "gathering.fetch_request",
    "gathering.fetch_bundle",
    "monitoring.inspect_function",
    "monitoring.inspect_instance",
    "gathering.plan_input",
    "lifecycle.end_execution",
    "runners.cron_trigger",
    "runners.destination_plugin",
    "runners.function",
    "runners.manifest_trigger",
    "runners.source_plugin",
    "runners.stream_source_plugin",
}


def versioned() -> list[str]:
    names = []
    prefix = f"{commands.__name__}."
    for module in pkgutil.walk_packages(commands.__path__, prefix):
        if not module.ispkg:
            continue
        command = importlib.import_module(module.name)
        if hasattr(command, "VERSION"):
            names.append(module.name.removeprefix(prefix))
    return sorted(names)


@pytest.mark.parametrize("name", versioned())
def test_command_declares_its_version_and_generation(name: str):
    command = importlib.import_module(f"{commands.__name__}.{name}")
    assert command.VERSION == version()
    assert command.GENERATION == (2 if name in SECOND_GENERATION else 1)
