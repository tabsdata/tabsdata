#
# Copyright 2026 Tabsdata Inc.
#

"""Every ``logical_prompt`` names the flag that supplies its value, so a run
that may not prompt fails with ``missing_argument`` and a "Pass <flag>." hint
instead."""

from __future__ import annotations

import ast
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

import tabsdatak.cli as cli_package
from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk.tabsdata_server import TabsdataServer

CLI_ROOT = Path(next(iter(cli_package.__path__)))

# Unreachable without a prompt: verify_login_or_prompt raises auth_required
# before logging in interactively, and pick_worker fails naming
# 'tdk worker logs --worker' before asking for an index.
WITHOUT_OPTION = {
    ("cli_utils", "request_login_information"),
    ("execution_utils", "pick_worker"),
}


def _flags(function: ast.FunctionDef) -> set[str]:
    flags = set()
    for decorator in function.decorator_list:
        if isinstance(decorator, ast.Call) and getattr(
            decorator.func, "attr", None
        ) in ("option", "argument"):
            flags.update(
                arg.value
                for arg in decorator.args
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str)
            )
    return flags


def _prompts():
    for path in sorted(CLI_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text())
        for function in ast.walk(tree):
            if not isinstance(function, ast.FunctionDef):
                continue
            for node in ast.walk(function):
                if (
                    isinstance(node, ast.Call)
                    and getattr(node.func, "id", None) == "logical_prompt"
                ):
                    yield path.stem, function, node


def _option(call: ast.Call) -> ast.expr | None:
    return next((k.value for k in call.keywords if k.arg == "option"), None)


def test_the_walk_finds_the_prompts():
    modules = {module for module, _, _ in _prompts()}
    assert {"cli_utils", "connection_group", "fn_group", "table_group"} <= modules


def test_every_prompt_names_its_option():
    missing = [
        f"{module}.{function.name}:{call.lineno}"
        for module, function, call in _prompts()
        if _option(call) is None and (module, function.name) not in WITHOUT_OPTION
    ]
    assert missing == []


def test_the_allow_list_is_still_needed():
    stale = WITHOUT_OPTION - {
        (module, function.name)
        for module, function, call in _prompts()
        if _option(call) is None
    }
    assert stale == set()


def test_named_options_are_flags_of_the_command():
    wrong = []
    for module, function, call in _prompts():
        option = _option(call)
        if not isinstance(option, ast.Constant):
            continue
        flag = option.value.split()[0]
        flags = _flags(function)
        if not (flag.startswith("--") or flag.startswith("<")) or (
            flags and flag not in flags
        ):
            wrong.append(f"{module}.{function.name}:{call.lineno} {option.value}")
    assert wrong == []


@pytest.mark.parametrize(
    "argv, env",
    [
        (["--no-prompt", "collection", "create"], {}),
        (["collection", "create"], {"TDK_AGENT": "1"}),
        (["collection", "create"], {"CLAUDECODE": "1"}),
    ],
)
def test_a_run_that_may_not_prompt_names_the_missing_option(
    tmp_path, monkeypatch, argv, env
):
    for name in ("TDK_AGENT", "CLAUDECODE", "GEMINI_CLI", "CODEX_THREAD_ID"):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)

    def _inject(ctx):
        ctx.obj["tabsdataserver"] = MagicMock(spec=TabsdataServer)

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        result = CliRunner().invoke(cli, argv)
    assert result.exit_code != 0
    shown = re.sub(r"[│\s]+", " ", result.output)
    assert "Missing required value: --name." in shown
    assert "Pass --name." in shown


@pytest.mark.parametrize(
    "argv, lookup",
    [
        (["table", "schema", "--name", "orders"], "tdk table list"),
        (["fn", "get", "--name", "enrich"], "tdk fn list"),
    ],
)
def test_a_missing_collection_says_how_to_find_it(tmp_path, monkeypatch, argv, lookup):
    monkeypatch.setenv("TDK_AGENT", "1")

    def _inject(ctx):
        ctx.obj["tabsdataserver"] = MagicMock(spec=TabsdataServer)

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        result = CliRunner().invoke(cli, argv)
    assert result.exit_code != 0
    shown = re.sub(r"[│\s]+", " ", result.output)
    assert "Missing required value: --coll." in shown
    assert f"'{lookup} --filter name:eq:<name>'" in shown
