#
# Copyright 2026 Tabsdata Inc.
#

"""``tdk connection`` CLI: the template/validate surface now speaks YAML."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from tabsdatak.cli.td.cli import cli


def _installed(type_: str, direction: str) -> bool:
    # noinspection PyProtectedMember
    from tabsdatak.cli import template

    try:
        template.resolve_conn(type_, direction)
        return True
    except template.TemplateError:
        return False


def test_types_lists_mode_and_system():
    result = CliRunner().invoke(cli, ["connection", "types"])
    assert result.exit_code == 0, result.output
    # New columns + a known reference connector (localfile always installed).
    assert "Mode" in result.output
    assert "System" in result.output
    assert "local-file-in" in result.output
    assert "Source" in result.output


def test_template_emits_yaml():
    result = CliRunner().invoke(cli, ["connection", "template", "-t", "local-file-in"])
    assert result.exit_code == 0, result.output
    assert "kind: connectionDef" in result.output
    # YAML, not JSON -- no leading brace.
    assert not result.output.lstrip().startswith("{")


def test_template_file_writes_document_to_path(tmp_path):
    # ``--file``/``-f`` writes the template to a file instead of stdout. The
    # template's output path is ``--file`` (the CLI-wide name), NOT ``-o`` --
    # ``-o/--output`` is reserved for the table/json/yaml format contract.
    out = tmp_path / "conn.yaml"
    result = CliRunner().invoke(
        cli, ["connection", "template", "-t", "local-file-in", "--file", str(out)]
    )
    assert result.exit_code == 0, result.output
    assert f"Wrote connection document to {out}" in result.output
    assert "kind: connectionDef" in out.read_text()
    # The short form -f is the same option.
    out2 = tmp_path / "conn2.yaml"
    result = CliRunner().invoke(
        cli, ["connection", "template", "-t", "local-file-in", "-f", str(out2)]
    )
    assert result.exit_code == 0, result.output
    assert out2.read_text() == out.read_text()


def test_template_file_dash_writes_to_stdout():
    # ``--file -`` means stdout, not a file literally named "-". The output is
    # identical to omitting ``--file``, with no "Wrote ..." status line.
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            cli, ["connection", "template", "-t", "local-file-in", "--file", "-"]
        )
        assert result.exit_code == 0, result.output
        assert "kind: connectionDef" in result.output
        assert "Wrote connection document" not in result.output
        assert not Path("-").exists()
    plain = CliRunner().invoke(cli, ["connection", "template", "-t", "local-file-in"])
    assert result.output == plain.output


def test_template_rejects_dash_o_output_flag():
    # ``-o`` no longer means "output file" here; it is not a template option, so
    # passing it is a usage error rather than silently writing a file named "x".
    result = CliRunner().invoke(
        cli, ["connection", "template", "-t", "local-file-in", "-o", "x"]
    )
    assert result.exit_code != 0
    assert "no such option" in result.output.lower()


@pytest.mark.skipif(
    not _installed("mysql-sql-in", "src"), reason="mysql connector not installed"
)
def test_template_yaml_carries_accepts_comments():
    result = CliRunner().invoke(cli, ["connection", "template", "-t", "mysql-sql-in"])
    assert result.exit_code == 0, result.output
    # uri (str|Secret) carries an "# accepts:" note in union order; the plain
    # arm shows the ``str:`` prefix that keeps it distinct from a secret.
    assert "# accepts: str:<value> | secret:<value>" in result.output


def test_validate_reads_yaml(tmp_path):
    # Generate a template (YAML), write it, validate it through the CLI.
    gen = CliRunner().invoke(cli, ["connection", "template", "-t", "local-file-in"])
    assert gen.exit_code == 0, gen.output
    path = tmp_path / "conn.yaml"
    path.write_text(gen.output)

    result = CliRunner().invoke(cli, ["connection", "validate", "-f", str(path)])
    assert result.exit_code == 0, result.output
    assert "Connection document is valid" in result.output


def test_validate_rejects_malformed_yaml(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("kind: : :\n  - not valid")
    result = CliRunner().invoke(cli, ["connection", "validate", "-f", str(path)])
    assert result.exit_code != 0
    assert "not valid YAML" in result.output


_ENV_REF_DOC = (
    "kind: connectionDef\n"
    "apiVersion: '1.0'\n"
    "type: tabsdatak.conn.localfile:LocalFileSrcConn\n"
    "spec:\n"
    "  base_path: '$secret:MY_BP'\n"
)


def test_validate_resolves_env_secret_refs(tmp_path, monkeypatch):
    # validate resolves `$secret:<ENV_VAR>` from the environment first, exactly
    # as create does -- so a document sourcing a secret from the environment
    # validates instead of being rejected as a bad secret value.
    path = tmp_path / "conn.yaml"
    path.write_text(_ENV_REF_DOC)
    monkeypatch.setenv("MY_BP", "/data/in")
    result = CliRunner().invoke(cli, ["connection", "validate", "-f", str(path)])
    assert result.exit_code == 0, result.output
    assert "Connection document is valid" in result.output
    assert "MY_BP" in result.output  # echoes the env var it read


def test_validate_reports_unset_env_secret_ref(tmp_path, monkeypatch):
    # An unset referenced var is reported by name, not silently rejected as a
    # malformed secret value.
    path = tmp_path / "conn.yaml"
    path.write_text(_ENV_REF_DOC)
    monkeypatch.delenv("MY_BP", raising=False)
    result = CliRunner().invoke(cli, ["connection", "validate", "-f", str(path)])
    assert result.exit_code != 0
    assert "MY_BP" in result.output
