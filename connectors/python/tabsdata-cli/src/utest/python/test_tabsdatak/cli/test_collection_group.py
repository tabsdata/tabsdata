#
# Copyright 2026 Tabs Data Inc.
#

import json

import click
import pytest

from tabsdatak.cli.td import collection_group


def _write_doc(tmp_path, document):
    path = tmp_path / "conn.json"
    path.write_text(json.dumps(document))
    return str(path)


def test_load_connection_document(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PASSWORD", "pw")
    path = _write_doc(
        tmp_path,
        {
            "kind": "mysqlSrcConn",
            "conn_configs": {
                "uri": "mysql://h/db",
                "credentials": {"env:password": "DB_PASSWORD"},
            },
        },
    )
    document, group, secrets = collection_group._load_connection_document(
        path, "invoices", None
    )
    assert document["name"] == "invoices"
    assert group == "source"
    assert secrets == [{"key": "DB_PASSWORD", "value": "pw"}]


def test_load_connection_document_group_mismatch(tmp_path):
    path = _write_doc(tmp_path, {"kind": "mysqlSrcConn", "conn_configs": {}})
    with pytest.raises(click.ClickException, match="source"):
        collection_group._load_connection_document(path, "x", "destination")


def test_load_connection_document_name_mismatch(tmp_path):
    path = _write_doc(
        tmp_path, {"kind": "mysqlDstConn", "name": "other", "conn_configs": {}}
    )
    with pytest.raises(click.ClickException, match="named by its collection"):
        collection_group._load_connection_document(path, "x", None)


def test_load_connection_document_missing_env(tmp_path, monkeypatch):
    monkeypatch.delenv("MISSING_KEY", raising=False)
    path = _write_doc(
        tmp_path,
        {"kind": "mysqlSrcConn", "conn_configs": {"env:p": "MISSING_KEY"}},
    )
    with pytest.raises(click.ClickException, match="MISSING_KEY"):
        collection_group._load_connection_document(path, "x", None)
