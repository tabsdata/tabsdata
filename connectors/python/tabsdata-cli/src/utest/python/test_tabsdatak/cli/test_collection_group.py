#
# Copyright 2026 Tabs Data Inc.
#

"""`tdk collection create --conn-file`: the connection is now a YAML `Conn`
document, validated and dehydrated client-side into the (connection, secrets)
the server receives."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import click
import pytest
from click.testing import CliRunner

from tabsdatak.cli.td import collection_group
from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk.tabsdata_server import TabsdataServer


def _installed(type_: str, direction: str) -> bool:
    # noinspection PyProtectedMember
    from tabsdatak.cli import template

    try:
        template.resolve_conn(type_, direction)
        return True
    except template.TemplateError:
        return False


_MYSQL_SRC_YAML = """\
kind: connectionDef
apiVersion: '1.0'
type: tabsdatak.conn.mysql:MySQLSrcConn
spec:
  uri: mysql://h:3306/db
  credentials:
    kind: userPasswordCredentials
    apiVersion: '1.0'
    type: tabsdatak.conn.common.types:UserPassword
    spec:
      user: secret:etl
      password: secret:hunter2
"""


def _write(tmp_path, text: str) -> str:
    path = tmp_path / "conn.yaml"
    path.write_text(text)
    return str(path)


@pytest.mark.skipif(
    not _installed("mysql-sql-in", "src"), reason="mysql connector not installed"
)
def test_load_connection_document_dehydrates(tmp_path):
    # noinspection PyProtectedMember
    from tabsdatak.cli import template

    connection, group, secrets = collection_group._load_connection_document(
        _write(tmp_path, _MYSQL_SRC_YAML), None
    )
    assert group == "sources"
    # The secret values are harvested out of the document, into the payload.
    assert {kv["value"] for kv in secrets} == {"etl", "hunter2"}
    # The connection sent is dehydrated YAML: secret leaves reduced to ":secret:",
    # plain identifiers left as-is.
    sent = template.from_yaml(connection)
    creds = sent["spec"]["credentials"]["spec"]
    assert creds["user"] == ":secret:"
    assert creds["password"] == ":secret:"
    assert sent["spec"]["uri"] == "mysql://h:3306/db"


# Same connection, but the secret values are supplied WITHOUT the `secret:`
# prefix. A `Secret`-typed field only accepts `secret:<value>` or `:secret:`,
# so a bare value is rejected at validation time -- the plain value can never
# reach the stored connection.
_MYSQL_SRC_YAML_BARE = """\
kind: connectionDef
apiVersion: '1.0'
type: tabsdatak.conn.mysql:MySQLSrcConn
spec:
  uri: mysql://h:3306/db
  credentials:
    kind: userPasswordCredentials
    apiVersion: '1.0'
    type: tabsdatak.conn.common.types:UserPassword
    spec:
      user: etl
      password: hunter2
"""


@pytest.mark.skipif(
    not _installed("mysql-sql-in", "src"), reason="mysql connector not installed"
)
def test_load_connection_document_rejects_unprefixed_secret_values(tmp_path):
    with pytest.raises(click.ClickException) as ei:
        collection_group._load_connection_document(
            _write(tmp_path, _MYSQL_SRC_YAML_BARE), None
        )
    # Both offending secret fields are named and the required `secret:` form
    # is spelled out; the load aborts, so nothing is stored or sent.
    message = str(ei.value)
    assert "credentials.user" in message
    assert "credentials.password" in message
    assert "secret:<value>" in message


# Secret values supplied as `$secret:<ENV_VAR>` are read from the environment
# at create time, then harvested like any other secret -- no `$secret:` leaf
# reaches the stored document.
_MYSQL_SRC_YAML_ENVREF = """\
kind: connectionDef
apiVersion: '1.0'
type: tabsdatak.conn.mysql:MySQLSrcConn
spec:
  uri: mysql://h:3306/db
  credentials:
    kind: userPasswordCredentials
    apiVersion: '1.0'
    type: tabsdatak.conn.common.types:UserPassword
    spec:
      user: $secret:DB_USER
      password: $secret:DB_PASS
"""


@pytest.mark.skipif(
    not _installed("mysql-sql-in", "src"), reason="mysql connector not installed"
)
def test_load_connection_document_resolves_env_secret_refs(
    tmp_path, monkeypatch, capsys
):
    # noinspection PyProtectedMember
    from tabsdatak.cli import template

    monkeypatch.setenv("DB_USER", "etl")
    monkeypatch.setenv("DB_PASS", "hunter2")
    connection, group, secrets = collection_group._load_connection_document(
        _write(tmp_path, _MYSQL_SRC_YAML_ENVREF), None
    )
    # The env-var values are harvested into the payload.
    assert {kv["value"] for kv in secrets} == {"etl", "hunter2"}
    sent = template.from_yaml(connection)
    creds = sent["spec"]["credentials"]["spec"]
    assert creds["user"] == ":secret:"
    assert creds["password"] == ":secret:"
    # The env vars read are echoed to the user at create time.
    out = capsys.readouterr().out
    assert "DB_USER" in out and "DB_PASS" in out


@pytest.mark.skipif(
    not _installed("mysql-sql-in", "src"), reason="mysql connector not installed"
)
def test_load_connection_document_env_secret_ref_missing_var(tmp_path, monkeypatch):
    monkeypatch.delenv("DB_USER", raising=False)
    monkeypatch.delenv("DB_PASS", raising=False)
    with pytest.raises(click.ClickException, match=r"\$secret:"):
        collection_group._load_connection_document(
            _write(tmp_path, _MYSQL_SRC_YAML_ENVREF), None
        )


@pytest.mark.skipif(
    not _installed("mysql-sql-in", "src"), reason="mysql connector not installed"
)
def test_load_connection_document_group_mismatch(tmp_path):
    # kind implies 'sources'; --group says 'destinations'.
    with pytest.raises(click.ClickException, match="sources"):
        collection_group._load_connection_document(
            _write(tmp_path, _MYSQL_SRC_YAML), "destinations"
        )


def test_load_connection_document_invalid_yaml(tmp_path):
    with pytest.raises(click.ClickException, match="invalid connection YAML"):
        collection_group._load_connection_document(
            _write(tmp_path, "kind: : :\n  - nope"), None
        )


@pytest.mark.skipif(
    not _installed("mysql-sql-in", "src"), reason="mysql connector not installed"
)
def test_group_for_conn():
    # The group is resolved from the registry binding (source/stream-source ->
    # sources, destination -> destinations), since every connection now shares
    # the uniform ``connectionDef`` kind.
    from tabsdatak.conn.mysql import (
        MySQLCdcStreamSrcConn,
        MySQLDestConn,
        MySQLSrcConn,
    )

    assert collection_group._group_for_conn(MySQLSrcConn) == "sources"
    assert collection_group._group_for_conn(MySQLDestConn) == "destinations"
    assert collection_group._group_for_conn(MySQLCdcStreamSrcConn) == "sources"

    class _Unregistered:
        pass

    with pytest.raises(click.ClickException):
        collection_group._group_for_conn(_Unregistered)


# --- `collection update --conn-file` upserts the connection ------------------
# The connection lives behind its own endpoint, so update goes through
# ``TabsdataServer.update_collection_connection`` (mocked here at the SDK seam,
# the same pattern as the mutating harness).


def _mock_client() -> MagicMock:
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    return client


def _invoke(argv, *, client, tmp_path):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with patch(
        "tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject
    ), patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)):
        return CliRunner().invoke(cli, argv)


@pytest.mark.skipif(
    not _installed("mysql-sql-in", "src"), reason="mysql connector not installed"
)
def test_update_conn_file_upserts_connection(tmp_path):
    conn = _write(tmp_path, _MYSQL_SRC_YAML)
    client = _mock_client()
    result = _invoke(
        ["collection", "update", "-n", "c", "--conn-file", conn],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert "Collection updated successfully" in result.output
    # Only --conn-file was given, so the scalar-update endpoint is skipped.
    client.update_collection.assert_not_called()
    client.update_collection_connection.assert_called_once()
    args, kwargs = client.update_collection_connection.call_args
    assert args[0] == "c"
    # Secret values are harvested into the (separate) secret payload.
    assert {kv["value"] for kv in kwargs["secrets"]} == {"etl", "hunter2"}
    # The connection is sent as the dehydrated YAML document (same wire form as
    # create): secret leaves reduced to ":secret:", no plaintext secret.
    connection = kwargs["connection"]
    assert ":secret:" in connection
    assert "hunter2" not in connection


@pytest.mark.skipif(
    not _installed("mysql-sql-in", "src"), reason="mysql connector not installed"
)
def test_update_conn_file_with_rename_updates_both(tmp_path):
    conn = _write(tmp_path, _MYSQL_SRC_YAML)
    client = _mock_client()
    result = _invoke(
        ["collection", "update", "-n", "c", "--new-name", "c2", "--conn-file", conn],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    # Scalar rename goes to update_collection; the connection upsert targets the
    # NEW name (the rename lands first).
    client.update_collection.assert_called_once()
    client.update_collection_connection.assert_called_once()
    assert client.update_collection_connection.call_args.args[0] == "c2"


def test_connection_view_calls_sdk_and_renders(tmp_path):
    client = _mock_client()
    client.get_collection_connection.return_value = {
        "id": "abc",
        "connection": {
            "type": "tabsdatak.conn.mysql:MySQLSrcConn",
            "spec": {"uri": "mysql://h:3306/db"},
        },
        "secret_names": ["S1", "S2"],
        "var_names": [],
    }
    result = _invoke(
        ["collection", "connection", "-n", "c", "-o", "json"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    client.get_collection_connection.assert_called_once_with("c")
    out = json.loads(result.output)
    assert out["connection"]["type"] == "tabsdatak.conn.mysql:MySQLSrcConn"
    assert out["secret_names"] == ["S1", "S2"]
