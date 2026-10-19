#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk.tabsdata_server import TabsdataServer

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

SECRETS_FILE = """\
aws:
  key1: mykey1
  key2: $secret:SECRETS_TEST_AWS_KEY2
  key3:
  key4: null
  key5: ""
warehouse:
  port: 0123
  enabled: yes
  quoted: "null"
"""


def _client() -> MagicMock:
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    return client


def _invoke(argv: list[str], client: MagicMock, tmp_path, stdin: str | None = None):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        return CliRunner().invoke(cli, argv, input=stdin)


def _secrets_file(tmp_path, content: str = SECRETS_FILE) -> str:
    path = tmp_path / "secrets.yaml"
    path.write_text(content, encoding="utf-8")
    return str(path)


def test_create_sends_the_secrets_of_the_file(tmp_path, monkeypatch):
    monkeypatch.setenv("SECRETS_TEST_AWS_KEY2", "mykey2")
    client = _client()
    result = _invoke(
        ["project", "create", "-n", "p1", "--secrets-file", _secrets_file(tmp_path)],
        client,
        tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert client.create_project.call_args.kwargs["secrets"] == {
        "aws": {
            "key1": "mykey1",
            "key2": "mykey2",
            "key3": None,
            "key4": None,
            "key5": "",
        },
        "warehouse": {"port": "0123", "enabled": "yes", "quoted": "null"},
    }
    assert "SECRETS_TEST_AWS_KEY2" in result.output
    assert "mykey2" not in result.output


def test_update_replaces_the_secrets_from_stdin(tmp_path):
    client = _client()
    result = _invoke(
        ["project", "update", "-n", "p1", "--secrets-file", "-"],
        client,
        tmp_path,
        stdin="warehouse:\n  password: p\n",
    )
    assert result.exit_code == 0, result.output
    assert client.update_project.call_args.kwargs["new_secrets"] == {
        "warehouse": {"password": "p"}
    }


def test_an_empty_file_clears_the_secrets(tmp_path):
    client = _client()
    result = _invoke(
        [
            "project",
            "update",
            "-n",
            "p1",
            "--secrets-file",
            _secrets_file(tmp_path, ""),
        ],
        client,
        tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert client.update_project.call_args.kwargs["new_secrets"] == {}


def test_an_unset_environment_variable_is_refused(tmp_path, monkeypatch):
    monkeypatch.delenv("SECRETS_TEST_AWS_KEY2", raising=False)
    client = _client()
    result = _invoke(
        ["project", "create", "-n", "p1", "--secrets-file", _secrets_file(tmp_path)],
        client,
        tmp_path,
    )
    assert result.exit_code != 0
    assert "SECRETS_TEST_AWS_KEY2" in result.output
    client.create_project.assert_not_called()


@pytest.mark.parametrize(
    "content",
    [
        "- aws\n",
        "aws: mykey1\n",
        "aws:\n  key1: [a, b]\n",
        "aws:\n  key1: {a: b}\n",
        "aws:\n  key1: a\nAWS:\n  key2: b\n",
        "aws:\n  key1: a\n  KEY1: b\n",
        "aws: [\n",
    ],
)
def test_malformed_files_are_refused(tmp_path, content):
    client = _client()
    result = _invoke(
        [
            "project",
            "create",
            "-n",
            "p1",
            "--secrets-file",
            _secrets_file(tmp_path, content),
        ],
        client,
        tmp_path,
    )
    assert result.exit_code != 0
    client.create_project.assert_not_called()


def test_secrets_lists_names_and_keys(tmp_path):
    client = _client()
    client.get_project_secrets.return_value = {"aws": ["key1", "key2"]}
    result = _invoke(["project", "secrets", "-n", "p1", "-o", "json"], client, tmp_path)
    assert result.exit_code == 0, result.output
    client.get_project_secrets.assert_called_once_with("p1")
    assert json.loads(result.output) == {"aws": ["key1", "key2"]}


def test_secrets_renders_a_table(tmp_path):
    client = _client()
    client.get_project_secrets.return_value = {"aws": ["key1", "key2"]}
    result = _invoke(["project", "secrets", "-n", "p1"], client, tmp_path)
    assert result.exit_code == 0, result.output
    assert "key1, key2" in result.output
    assert "Number of secrets: 1" in result.output
