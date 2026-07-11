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
    from tabsdatak import _template as template

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
# prefix. They are `Secret`-typed fields, so dehydration must still harvest
# them (driven by the validated model, not by the prefix) -- otherwise the
# plain values would leak into the stored connection.
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
def test_load_connection_document_dehydrates_unprefixed_secret_values(tmp_path):
    # noinspection PyProtectedMember
    from tabsdatak import _template as template

    connection, group, secrets = collection_group._load_connection_document(
        _write(tmp_path, _MYSQL_SRC_YAML_BARE), None
    )
    # Bare values are harvested just like prefixed ones -- no plaintext leak.
    assert {kv["value"] for kv in secrets} == {"etl", "hunter2"}
    sent = template.from_yaml(connection)
    creds = sent["spec"]["credentials"]["spec"]
    assert creds["user"] == ":secret:"
    assert creds["password"] == ":secret:"
    # `uri` resolves to the str member of `str | Secret`, so it is NOT a secret.
    assert sent["spec"]["uri"] == "mysql://h:3306/db"


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
    from tabsdatak import _template as template

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
