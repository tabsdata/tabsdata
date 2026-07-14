"""db2 domains are SCHEMAS inside one shared 'XP' database (db names cap at 8
chars, too short for the prefixed domain names; schemas allow 128). We connect
as the instance user (db2inst1) with currentSchema pinned — no per-domain OS
user (DB2's OS-auth for dynamic users is unreliable in the community image)."""
from __future__ import annotations

from tabsdatax.itest.tdksystems.systems._container import AdminConn
from tabsdatax.itest.tdksystems.systems.db2.system import Db2System, XP_DB, db2_schema_name


def _admin() -> AdminConn:
    return AdminConn(host="localhost", port="55391",
                     user="db2inst1", password="instpw")


def test_db2_schema_name_uppercases_without_truncating():
    assert db2_schema_name("db2_IN") == "DB2_IN"
    # No 8-char cap — the full prefixed name fits a 128-char schema name.
    assert (db2_schema_name("D_20260615T091316Z_tucu_db2_IN")
            == "D_20260615T091316Z_TUCU_DB2_IN")


def test_do_create_makes_a_schema_in_the_shared_db(monkeypatch):
    sysobj = Db2System()
    statements: list[str] = []
    # Admin runs over TCP (no docker exec, no belt-and-braces _post_start).
    monkeypatch.setattr(sysobj, "_run_admin_sql",
                        lambda admin, stmts: statements.extend(stmts))

    # domain_result computes the (admin-credentialled) identity; do_create runs
    # the schema DDL for it.
    res = sysobj.domain_result(_admin(), "D_20260615T091316Z_tucu_db2_IN", "unused")
    schema = "D_20260615T091316Z_TUCU_DB2_IN"

    # Connect as the instance user, into the shared XP database, schema pinned.
    assert res.user == "db2inst1"
    assert res.password == "instpw"
    assert res.db == schema
    assert f"db2inst1:instpw@localhost:55391/{XP_DB}?currentSchema={schema}" in res.sqlalchemy_uri

    sysobj.do_create(_admin(), res)
    joined = "\n".join(statements)
    assert f"CREATE SCHEMA {schema}" in joined
    # No per-domain OS user, and no per-domain database.
    for forbidden in ("useradd", "chpasswd", "CREATE DATABASE", "create database"):
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


def test_do_delete_drops_objects_then_the_schema(monkeypatch):
    sysobj = Db2System()
    schema = "D_20260615T091316Z_TUCU_DB2_IN"
    statements: list[str] = []
    monkeypatch.setattr(sysobj, "_run_admin_sql",
                        lambda admin, stmts: statements.extend(stmts))
    # the schema exists (do_list), with one view + two tables; dropped last.
    monkeypatch.setattr(
        sysobj, "_query_admin_names",
        lambda admin, sql: (["V1"] if "syscat.views" in sql
                            else ["T1", "T2"] if "syscat.tables" in sql
                            else [schema] if "syscat.schemata" in sql else []))
    sysobj.do_delete(_admin(), "D_20260615T091316Z_tucu_db2_IN")
    assert f"DROP VIEW {schema}.V1" in statements
    assert f"DROP TABLE {schema}.T1" in statements
    assert f"DROP TABLE {schema}.T2" in statements
    # the (now empty) schema is dropped last, with RESTRICT (no admin routine).
    assert statements[-1] == f"DROP SCHEMA {schema} RESTRICT"
    assert not any("ADMIN_DROP_SCHEMA" in s for s in statements)


def test_do_delete_is_idempotent_when_schema_absent(monkeypatch):
    # destroy of an absent schema (e.g. a stale descriptor) is a clean no-op —
    # no DROP is attempted (which would raise a noisy SQL0204N).
    sysobj = Db2System()
    statements: list[str] = []
    monkeypatch.setattr(sysobj, "_run_admin_sql",
                        lambda admin, stmts: statements.extend(stmts))
    monkeypatch.setattr(sysobj, "_query_admin_names", lambda admin, sql: [])
    sysobj.do_delete(_admin(), "gone")
    assert statements == []
