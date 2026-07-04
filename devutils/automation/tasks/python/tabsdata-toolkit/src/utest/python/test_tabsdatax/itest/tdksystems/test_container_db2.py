"""db2 domains are SCHEMAS inside one shared 'XP' database (db names cap at 8
chars, too short for the prefixed domain names; schemas allow 128). We connect
as the instance user (db2inst1) with currentSchema pinned — no per-domain OS
user (DB2's OS-auth for dynamic users is unreliable in the community image)."""
from __future__ import annotations

from tabsdatax.itest.tdksystems.systems.db2.system import Db2System, XP_DB, db2_schema_name


def test_db2_schema_name_uppercases_without_truncating():
    assert db2_schema_name("db2_IN") == "DB2_IN"
    # No 8-char cap — the full prefixed name fits a 128-char schema name.
    assert (db2_schema_name("D_20260615T091316Z_tucu_db2_IN")
            == "D_20260615T091316Z_TUCU_DB2_IN")


def test_do_create_makes_a_schema_in_the_shared_db(monkeypatch):
    sysobj = Db2System()
    scripts: list[str] = []
    monkeypatch.setattr(sysobj, "_list_databases", lambda c: [XP_DB])  # XP exists
    monkeypatch.setattr(
        sysobj, "_exec_inst",
        lambda container, script, ignore_failure=False: scripts.append(script) or "",
    )
    monkeypatch.setenv("PORT", "55391")

    res = sysobj.do_create("db2", "instpw", "D_20260615T091316Z_tucu_db2_IN")
    schema = "D_20260615T091316Z_TUCU_DB2_IN"

    # Connect as the instance user, into the shared XP database, schema pinned.
    assert res.user == "db2inst1"
    assert res.password == "instpw"
    assert res.db == schema
    assert f"db2inst1:instpw@localhost:55391/{XP_DB}?currentSchema={schema}" in res.sqlalchemy_uri

    joined = "\n".join(scripts)
    assert f"connect to {XP_DB}" in joined
    assert f"CREATE SCHEMA {schema}" in joined
    # No per-domain OS user, and no per-domain database.
    for forbidden in ("useradd", "chpasswd", "create database D_"):
        assert forbidden not in joined, f"db2 do_create should not run {forbidden!r}"


def test_post_start_creates_shared_db_when_missing(monkeypatch):
    sysobj = Db2System()
    scripts: list[str] = []
    monkeypatch.setattr(sysobj, "_list_databases", lambda c: [])  # XP missing
    monkeypatch.setattr(
        sysobj, "_exec_inst",
        lambda container, script, ignore_failure=False: scripts.append(script) or "",
    )
    sysobj._post_start("db2", "instpw")
    assert any(f"create database {XP_DB}" in s for s in scripts)


def test_post_start_skips_when_shared_db_exists(monkeypatch):
    sysobj = Db2System()
    scripts: list[str] = []
    monkeypatch.setattr(sysobj, "_list_databases", lambda c: [XP_DB])
    monkeypatch.setattr(
        sysobj, "_exec_inst",
        lambda container, script, ignore_failure=False: scripts.append(script) or "",
    )
    sysobj._post_start("db2", "instpw")
    assert scripts == []


def test_do_delete_drops_the_schema(monkeypatch):
    sysobj = Db2System()
    scripts: list[str] = []
    monkeypatch.setattr(
        sysobj, "_exec_inst",
        lambda container, script, ignore_failure=False: scripts.append(script) or "",
    )
    sysobj.do_delete("db2", "instpw", "D_20260615T091316Z_tucu_db2_IN")
    joined = "\n".join(scripts)
    assert "ADMIN_DROP_SCHEMA('D_20260615T091316Z_TUCU_DB2_IN'" in joined
