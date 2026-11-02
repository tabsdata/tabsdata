#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json

import rich_click as click
from click.testing import CliRunner

from tabsdatak.cli import _registry
from tabsdatak.cli._machine import Machine, machine, running
from tabsdatak.cli.td.cli import cli


class _Recording:
    def __init__(self, name: str, calls: list, **overrides):
        self.name, self.calls, self.overrides = name, calls, overrides

    def main(self, root, argv, run):
        self.calls.append((self.name, list(argv)))
        return run(argv, **self.overrides)

    def root_params(self):
        return []

    def describe_root(self) -> dict:
        return {f"{self.name}_root": True}

    def describe(self, command, path) -> dict:
        return {f"{self.name}_leaf": path}


def test_extensions_wrap_in_registration_order_and_pass_overrides(monkeypatch):
    calls = []
    outer = _Recording("outer", calls, standalone_mode=False)
    inner = _Recording("inner", calls)
    monkeypatch.setattr(_registry, "_CLI_EXTENSIONS", [outer, inner])
    assert cli.main(["--version"], standalone_mode=True) == 0
    assert calls == [("outer", ["--version"]), ("inner", ["--version"])]


def test_extensions_add_to_the_full_help_tree(monkeypatch):
    calls = []
    monkeypatch.setattr(_registry, "_CLI_EXTENSIONS", [_Recording("x", calls)])
    result = CliRunner().invoke(cli, ["--help", "--full", "--format", "json"])
    tree = json.loads(result.stdout)
    assert tree["x_root"] is True
    auth = next(node for node in tree["commands"] if node["name"] == "auth")
    info = next(node for node in auth["commands"] if node["name"] == "info")
    assert info["x_leaf"] == "auth info"


def test_a_machine_run_emits_the_result_instead_of_printing_it(monkeypatch):
    emitted = []

    class _Driver(_Recording):
        def main(self, root, argv, run):
            with running(Machine(emit=emitted.append, command=["tdk", *argv])):
                return run(argv, standalone_mode=False)

    seen = {}

    @click.command("probe")
    @click.pass_context
    def probe(ctx):
        from tabsdatak.cli.cli_utils import render

        seen["machine"] = machine()
        ctx.obj["output_format"] = "json"
        render(ctx, data={"ok": True})

    monkeypatch.setattr(_registry, "_CLI_EXTENSIONS", [_Driver("d", [])])
    monkeypatch.setitem(cli.commands, "probe", probe)
    result = CliRunner().invoke(cli, ["probe"])
    assert result.exit_code == 0, result.output
    assert emitted == [{"ok": True}]
    assert result.stdout == ""
    assert seen["machine"].command == ["tdk", "probe"]
    assert machine() is None
