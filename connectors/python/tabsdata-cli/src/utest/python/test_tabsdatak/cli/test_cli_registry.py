#
# Copyright 2026 Tabs Data Inc.
#

"""Unit tests for the tdk CLI command registry / auto-collection seam.

Covers :mod:`tabsdatak.cli._registry`: the ``@cli_command`` annotation, the
``collect_cli_commands`` collector, and its best-effort, never-fatal import of
the enterprise variant's command module. Also checks the built-in groups reach
the root ``tdk`` CLI through that registry rather than a manual ``add_command``
list.
"""

from __future__ import annotations

import rich_click as click

from tabsdatak.cli import _registry
from tabsdatak.cli._registry import (
    _CLI_COMMAND_REGISTRY,
    cli_command,
    collect_cli_commands,
)

# role / cron / stage are temporarily removed from the root CLI (see the TODO
# in tabsdatak.cli.td.cli); re-add them here when their modules return.
BUILTIN_GROUPS = {
    "auth",
    "collection",
    "connection",
    "exe",
    "fn",
    "group",
    "metrics",
    "permission",
    "pin",
    "project",
    "table",
    "user",
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


def test_collect_skips_missing_variant_module(monkeypatch):
    # No enterprise variant installed: the import target is absent. collect must
    # not raise -- it returns the (built-in) registry unchanged.
    monkeypatch.setattr(
        _registry,
        "_VARIANT_CLI_COMMANDS",
        "tabsdatak.extensions.variant.cli.__does_not_exist__",
    )
    collected = collect_cli_commands()
    assert isinstance(collected, list)


def test_collect_survives_broken_variant_module(monkeypatch):
    # A variant module that blows up on import (not just ImportError) is logged
    # and skipped, never breaking the core CLI.
    def boom(_name):
        raise RuntimeError("variant import exploded")

    monkeypatch.setattr(_registry.importlib, "import_module", boom)
    collected = collect_cli_commands()  # must not propagate the RuntimeError
    assert isinstance(collected, list)


def test_builtin_groups_reach_tdk_via_registry():
    # Importing the root CLI annotates + collects every built-in group.
    from tabsdatak.cli.td.cli import cli

    missing = BUILTIN_GROUPS - set(cli.commands)
    assert not missing, f"built-in groups missing from `tdk`: {sorted(missing)}"
    # They arrived through the @cli_command registry, not a manual add_command.
    assert BUILTIN_GROUPS <= {c.name for c in _CLI_COMMAND_REGISTRY}
