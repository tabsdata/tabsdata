#
# Copyright 2026 Tabsdata Inc.
#

"""Smoke tests for the rich-click CLI."""

from click.testing import CliRunner

from tabsdatax.generator.tableframe.cli import main

runner = CliRunner()


def _flat(text: str) -> str:
    # Rich-click wraps help/usage across lines; collapse whitespace so substring
    # checks don't depend on where a token happens to wrap.
    return " ".join(text.split())


def test_no_subcommand_shows_help_and_does_not_crash():
    result = runner.invoke(main, [])
    assert result.exit_code != 0
    flat = _flat(result.output)
    assert "inspect" in flat and "generate" in flat


def test_subcommands_are_registered():
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    flat = _flat(result.output)
    for sub in ("inspect", "compare", "generate"):
        assert sub in flat


def test_inspect_help_exposes_manifest_and_module():
    flat = _flat(runner.invoke(main, ["inspect", "--help"]).output)
    assert "--manifest" in flat and "--module" in flat


def test_inspect_without_manifest_or_env_errors(monkeypatch):
    monkeypatch.delenv("TABSDATA_OS", raising=False)
    result = runner.invoke(main, ["inspect", "--yes"])
    assert result.exit_code != 0


def test_compare_requires_old_and_new():
    result = runner.invoke(main, ["compare", "--old", "a.json"])
    assert result.exit_code != 0
    flat = _flat(runner.invoke(main, ["compare", "--help"]).output)
    assert "--old" in flat and "--new" in flat


def test_generate_help_exposes_manifest_module_package():
    flat = _flat(runner.invoke(main, ["generate", "--help"]).output)
    assert "--manifest" in flat
    assert "--module" in flat
    assert "--package" in flat
