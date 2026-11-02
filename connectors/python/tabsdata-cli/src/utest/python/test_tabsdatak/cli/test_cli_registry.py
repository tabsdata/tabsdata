#
# Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the tdk CLI command registry / auto-collection seam.

Covers :mod:`tabsdatak.cli._registry`: the ``@cli_command`` annotation, the
``collect_cli_commands`` collector, and its import of the enterprise variant's
command module: an absent variant is skipped, a broken one propagates. Also
checks the built-in groups reach the root ``tdk`` CLI through that registry
rather than a manual ``add_command`` list.
"""

from __future__ import annotations

import pytest
import rich_click as click

from tabsdatak.cli import _registry
from tabsdatak.cli._registry import (
    _CLI_COMMAND_REGISTRY,
    cli_command,
    collect_cli_commands,
)

# The core command groups the open-source ``tdk`` CLI must always expose. Kept
# in lockstep with what ``tabsdatak.cli.td.cli`` imports for its @cli_command
# side effects. Add a group here when it is wired into ``cli.py`` so this guards
# against a group silently dropping out of the registry.
BUILTIN_GROUPS = {
    "auth",
    "collection",
    "connection",
    "cron",
    "fn",
    "fnrun",
    "group",
    "metrics",
    "permission",
    "plan",
    "project",
    "set",
    "settings",
    "stream",
    "table",
    "trx",
    "unset",
    "user",
    "worker",
}


def test_cli_command_registers_and_returns_unchanged():
    before = len(_CLI_COMMAND_REGISTRY)

    @cli_command
    @click.group(name="probe")
    def probe():
        """throwaway group used only to assert registration"""

    try:
        assert probe.name == "probe"  # returned unchanged, still usable
        assert _CLI_COMMAND_REGISTRY[-1] is probe
        assert len(_CLI_COMMAND_REGISTRY) == before + 1
    finally:
        # keep the module-global registry clean for other tests
        _CLI_COMMAND_REGISTRY.remove(probe)


def test_collect_includes_registered_commands():
    @cli_command
    @click.group(name="probe2")
    def probe2():
        """throwaway"""

    try:
        assert probe2 in collect_cli_commands()
    finally:
        _CLI_COMMAND_REGISTRY.remove(probe2)


@pytest.mark.parametrize(
    "missing",
    [
        "tabsdatak.extensions",
        "tabsdatak.extensions.variant",
        "tabsdatak.extensions.variant.cli",
        "tabsdatak.extensions.variant.cli.commands",
    ],
)
def test_collect_skips_an_absent_variant(monkeypatch, missing):
    def absent(_name):
        raise ModuleNotFoundError(f"No module named '{missing}'", name=missing)

    monkeypatch.setattr(_registry.importlib, "import_module", absent)
    assert collect_cli_commands() == list(_CLI_COMMAND_REGISTRY)


@pytest.mark.parametrize(
    "error",
    [
        ModuleNotFoundError("No module named 'tabsdatak.ag'", name="tabsdatak.ag"),
        ModuleNotFoundError(
            "No module named 'tabsdatak.extensions.variant.cli.commandsx'",
            name="tabsdatak.extensions.variant.cli.commandsx",
        ),
        ImportError("cannot import name 'classify'", name="tabsdatak.x"),
        RuntimeError("variant import exploded"),
    ],
)
def test_collect_propagates_a_broken_variant(monkeypatch, error):
    def broken(_name):
        raise error

    monkeypatch.setattr(_registry.importlib, "import_module", broken)
    with pytest.raises(type(error)) as raised:
        collect_cli_commands()
    assert raised.value is error


def test_builtin_groups_reach_tdk_via_registry():
    # Importing the root CLI annotates + collects every built-in group.
    from tabsdatak.cli.td.cli import cli

    missing = BUILTIN_GROUPS - set(cli.commands)
    assert not missing, f"built-in groups missing from `tdk`: {sorted(missing)}"
    # They arrived through the @cli_command registry, not a manual add_command.
    assert BUILTIN_GROUPS <= {c.name for c in _CLI_COMMAND_REGISTRY}
