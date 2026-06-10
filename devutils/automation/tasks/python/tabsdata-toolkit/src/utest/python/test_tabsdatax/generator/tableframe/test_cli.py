"""Smoke tests for the rich-click CLI skeleton."""

from click.testing import CliRunner

from tabsdatax.generator.tableframe.cli import main

runner = CliRunner()


def _flat(text: str) -> str:
    # Rich-click wraps help/usage across lines; collapse whitespace so substring
    # checks don't depend on where a token happens to wrap.
    return " ".join(text.split())


def test_no_subcommand_shows_help_and_does_not_crash():
    # a bare group invocation exits non-zero (usage), not with a traceback
    result = runner.invoke(main, [])
    assert result.exit_code != 0
    flat = _flat(result.output)
    assert "manifest" in flat and "generate" in flat


def test_subcommands_are_registered():
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    flat = _flat(result.output)
    for sub in ("manifest", "compare", "generate"):
        assert sub in flat


def test_manifest_requires_root_module():
    result = runner.invoke(main, ["manifest"])
    assert result.exit_code != 0


def test_manifest_rejects_root_dir():
    # manifest never accepted --root-dir (it describes the surface, not where it lands)
    result = runner.invoke(main, ["manifest", "--root-module", "m", "--root-dir", "d"])
    assert result.exit_code != 0


def test_compare_requires_old_and_new_manifest():
    # only --old-manifest given -> usage error; --new-manifest is a known option
    result = runner.invoke(main, ["compare", "--old-manifest", "a.json"])
    assert result.exit_code != 0
    assert "--new-manifest" in _flat(runner.invoke(main, ["compare", "--help"]).output)


def test_generate_requires_manifest():
    result = runner.invoke(main, ["generate", "--root-module", "m", "--root-dir", "d"])
    assert result.exit_code != 0
    assert "--manifest" in _flat(runner.invoke(main, ["generate", "--help"]).output)
