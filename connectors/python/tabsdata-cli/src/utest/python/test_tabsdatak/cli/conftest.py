#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging

import pytest

from tabsdatak.cli._unattended import UNATTENDED_MARKERS

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@pytest.fixture(autouse=True)
def _neutralize_tdk_env(monkeypatch):
    # TDK_* credentials turn stateless mode on, and an agent's markers turn
    # prompting off; clear them so the suite runs the same wherever it runs.
    for var in (
        "TDK_BEARER",
        "TDK_TOKEN",
        "TDK_SERVER",
        "TDK_PROJECT",
        "TDK_AGENT",
        *UNATTENDED_MARKERS,
    ):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture(autouse=True)
def _neutralize_forced_terminal(monkeypatch):
    # rich-click forces a styled terminal when FORCE_COLOR, PY_COLORS or
    # GITHUB_ACTIONS is set, deciding it once at import, and rich honours
    # FORCE_COLOR on every console; their ANSI codes would split the messages
    # the tests look for.
    monkeypatch.setattr("rich_click.rich_click.FORCE_TERMINAL", None)
    for var in ("FORCE_COLOR", "PY_COLORS"):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture(autouse=True)
def _machine_double(monkeypatch):
    # Only the test double drives tdk here, whatever extensions are installed.
    from tabsdatak.cli import _registry
    from test_tabsdatak.cli import machine_double

    monkeypatch.setattr(
        _registry, "_CLI_EXTENSIONS", [machine_double.MachineDouble()]
    )
    monkeypatch.setattr(machine_double, "SETTINGS", {})


@pytest.fixture
def machine_settings():
    """The Machine fields, beyond ``emit`` and ``command``, of every
    ``--machine`` run in the test."""
    from test_tabsdatak.cli import machine_double

    return machine_double.SETTINGS

from test_tabsdatak.bootest import enrich_sys_path
from test_tabsdatak.cli.bootest import TESTING_RESOURCES_PATH


def _enrich_sys_path():
    pass


TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
_enrich_sys_path()

# noinspection PyUnusedImports
