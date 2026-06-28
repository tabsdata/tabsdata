#
# Copyright 2026 Tabs Data Inc.
#

"""`tdk collection create --conn-file`: the connection is now a YAML `Conn`
document, validated and dehydrated client-side into the (connection, secrets)
the server receives."""

from __future__ import annotations

import click
import pytest

from tabsdatak.cli.td import collection_group


def _installed(type_: str, direction: str) -> bool:
    # noinspection PyProtectedMember
    from tabsdatak import _template as template

    try:
        template.resolve_conn(type_, direction)
        return True
    except template.TemplateError:
        return False


_MYSQL_SRC_YAML = """\
kind: mysqlSrcConn
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
    not _installed("mysql", "src"), reason="mysql connector not installed"
)
def test_load_connection_document_dehydrates(tmp_path):
    # noinspection PyProtectedMember
    from tabsdatak import _template as template

    connection, group, secrets = collection_group._load_connection_document(
        _write(tmp_path, _MYSQL_SRC_YAML), None
    )
    assert group == "source"
    # The secret values are harvested out of the document, into the payload.
    assert {kv["value"] for kv in secrets} == {"etl", "hunter2"}
    # The connection sent is dehydrated YAML: secret leaves reduced to ":secret:",
    # plain identifiers left as-is.
    sent = template.from_yaml(connection)
    creds = sent["spec"]["credentials"]["spec"]
    assert creds["user"] == ":secret:"
    assert creds["password"] == ":secret:"
    assert sent["spec"]["uri"] == "mysql://h:3306/db"


@pytest.mark.skipif(
    not _installed("mysql", "src"), reason="mysql connector not installed"
)
def test_load_connection_document_group_mismatch(tmp_path):
    # kind implies 'source'; --group says 'destination'.
    with pytest.raises(click.ClickException, match="source"):
        collection_group._load_connection_document(
            _write(tmp_path, _MYSQL_SRC_YAML), "destination"
        )


def test_load_connection_document_invalid_yaml(tmp_path):
    with pytest.raises(click.ClickException, match="invalid connection YAML"):
        collection_group._load_connection_document(
            _write(tmp_path, "kind: : :\n  - nope"), None
        )


def test_group_for_kind():
    assert collection_group._group_for_kind("mysqlSrcConn") == "source"
    assert collection_group._group_for_kind("mysqlDestConn") == "destination"
    assert collection_group._group_for_kind("mysqlCdcStreamSrcConn") == "source"
    with pytest.raises(click.ClickException):
        collection_group._group_for_kind("notAConn")
