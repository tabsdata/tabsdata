#
# Copyright 2026 Tabs Data Inc.
#

"""``tdk connection`` CLI: the template/validate surface now speaks YAML."""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from tabsdatak.cli.td.cli import cli


def _installed(type_: str, direction: str) -> bool:
    # noinspection PyProtectedMember
    from tabsdatak import _template as template

    try:
        template.resolve_conn(type_, direction)
        return True
    except template.TemplateError:
        return False


def test_template_emits_yaml():
    result = CliRunner().invoke(cli, ["connection", "template", "-t", "localfile", "-d", "src"])
    assert result.exit_code == 0, result.output
    assert "kind: localFileSrcConn" in result.output
    # YAML, not JSON -- no leading brace.
    assert not result.output.lstrip().startswith("{")


@pytest.mark.skipif(
    not _installed("mysql", "src"), reason="mysql connector not installed"
)
def test_template_yaml_carries_accepts_comments():
    result = CliRunner().invoke(cli, ["connection", "template", "-t", "mysql", "-d", "src"])
    assert result.exit_code == 0, result.output
    # uri (str|Var|Secret) carries an "# accepts:" note in union order.
    assert "# accepts: <value> | var:<value> | secret:<value>" in result.output


def test_validate_reads_yaml(tmp_path):
    # Generate a template (YAML), write it, validate it through the CLI.
    gen = CliRunner().invoke(cli, ["connection", "template", "-t", "localfile", "-d", "src"])
    assert gen.exit_code == 0, gen.output
    path = tmp_path / "conn.yaml"
    path.write_text(gen.output)

    result = CliRunner().invoke(cli, ["connection", "validate", "-f", str(path)])
    assert result.exit_code == 0, result.output
    assert "is valid (localFileSrcConn)" in result.output


def test_validate_rejects_malformed_yaml(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("kind: : :\n  - not valid")
    result = CliRunner().invoke(cli, ["connection", "validate", "-f", str(path)])
    assert result.exit_code != 0
    assert "not valid YAML" in result.output
