#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for the saas-sql data create / load path (databricks / snowflake),
with the network-facing `exec_statements` mocked. Verifies the SQL-capability
gate, DDL table discovery, DDL parity, and the generated DDL/INSERT statements."""
from __future__ import annotations

from unittest import mock

from tabsdatax.itest.tdksystems import data_gen, systems
from tabsdatax.itest.tdksystems.systems._saas import SaasBase
from tabsdatax.itest.tdksystems.systems.base import CreateDomainSpec  # noqa: F401 (parity)
from tabsdatax.itest.tdksystems.systems.base import DomainContext, LoadRequest
from tabsdatax.itest.tdksystems.systems.databricks.system import DatabricksSystem

DB_META = {
    "KIND": "saas-domain", "SAAS_TYPE": "databricks", "SYSTEM_KIND": "sql",
    "SCHEMA": "world_in", "HOST": "h", "TOKEN": "t",
    "WAREHOUSE_ID": "w", "CATALOG": "c", "VOLUME": "v",
}


def _db_ctx() -> DomainContext:
    return DomainContext("databricks", "world_in", DB_META)


def test_supports_sql_gate():
    assert systems.get("databricks").supports_sql is True
    assert systems.get("snowflake").supports_sql is True
    assert systems.get("salesforce").supports_sql is False


def test_data_create_is_a_noop_for_kafka(monkeypatch):
    # Kafka topics have no schema — `data create` must skip (return 0), not raise
    # "no SQL backend", so a kafka domain can still be seeded via create/--auto
    # (the records are produced by `data load`).
    import argparse

    from tabsdatax.itest.tdksystems import cli

    monkeypatch.setattr(cli.env_check, "check_requirements", lambda: None)
    meta = {"KIND": "container-domain", "SYSTEM_KIND": "kafka"}
    monkeypatch.setattr(cli, "_domain", lambda dp: (systems.get("redpanda"), None, meta))
    rc = cli._run_data_create(
        argparse.Namespace(model="world", domain="x", force=False)
    )
    assert rc == 0


def test_ddl_table_names():
    stmts = [
        "CREATE TABLE foo (a int)",
        "-- comment\nCREATE TABLE IF NOT EXISTS `bar` (b int)",
        "SELECT 1",
    ]
    assert SaasBase._ddl_table_names(stmts) == ["foo", "bar"]


def test_every_model_with_databricks_ddl_ships_a_matching_snowflake_ddl():
    """Snowflake mirrors Databricks: every model that ships a databricks.ddl
    must ship a snowflake.ddl with the same tables and no Databricks-only
    syntax (GENERATED ... IDENTITY, backtick quoting)."""
    from tabsdatax.itest.tdksystems.sql_runner import split_sql_statements
    checked = 0
    for model in data_gen.list_models():
        dbk = data_gen.model_dir(model) / "databricks.ddl"
        if not dbk.is_file():
            continue
        checked += 1
        snow = data_gen.model_dir(model) / "snowflake.ddl"
        assert snow.is_file(), f"{model}: missing snowflake.ddl"
        snow_text = snow.read_text()
        assert "GENERATED ALWAYS AS IDENTITY" not in snow_text, \
            f"{model}: snowflake.ddl uses Databricks IDENTITY syntax"
        assert "`" not in snow_text, \
            f"{model}: snowflake.ddl uses backtick quoting (use double quotes)"
        db_tables = SaasBase._ddl_table_names(split_sql_statements(dbk.read_text()))
        sf_tables = SaasBase._ddl_table_names(split_sql_statements(snow_text))
        assert sf_tables == db_tables, \
            f"{model}: snowflake.ddl tables {sf_tables} != databricks {db_tables}"
    assert checked > 0, "no models with a databricks.ddl were found"


def test_install_schema_force_drops_then_creates():
    calls = []
    with mock.patch.object(
        DatabricksSystem, "exec_statements",
        lambda self, ctx, stmts: calls.append((ctx.meta.get("SCHEMA"), list(stmts))),
    ):
        systems.get("databricks").install_schema(_db_ctx(), "world", force=True)
    # 1st call = the DROPs, 2nd = the CREATEs, both in the domain schema.
    assert calls[0][0] == "world_in"
    assert all(s.startswith("DROP TABLE IF EXISTS") for s in calls[0][1])
    assert {s.split()[-1] for s in calls[0][1]} == {
        "wld_country", "wld_city", "wld_country_language",
    }
    assert any("CREATE TABLE" in s.upper() for s in calls[1][1])


def test_load_data_builds_inserts(tmp_path):
    out = tmp_path / "d"
    data_gen.generate(data_gen.model_dir("world"), "xs", out, "json",
                      schema_sidecar=True)
    calls = []
    with mock.patch.object(
        DatabricksSystem, "exec_statements",
        lambda self, ctx, stmts: calls.append((ctx.meta.get("SCHEMA"), list(stmts))),
    ):
        systems.get("databricks").load_data(
            _db_ctx(), LoadRequest(model="world", data_dir=out, size="xs"))
    assert len(calls) == 3  # one exec per table
    for schema, stmts in calls:
        assert schema == "world_in"
        assert stmts and all(s.startswith("INSERT INTO ") for s in stmts)


def test_salesforce_has_no_sql_schema():
    # Salesforce is not SQL-capable: install_schema refuses (cli prints the
    # "objects already exist" notice instead of calling it).
    sf = systems.get("salesforce")
    assert sf.supports_sql is False
    ctx = DomainContext("salesforce", "sf", {"KIND": "saas-domain"})
    try:
        sf.install_schema(ctx, "salesforce")
        assert False, "expected install_schema to refuse"
    except Exception as exc:
        assert "schema" in str(exc).lower()


def test_databricks_create_schema_creates_schema_and_volume(monkeypatch):
    from tabsdatax.itest.tdksystems.systems.databricks import connection

    sysobj = DatabricksSystem()
    sql: list[str] = []
    monkeypatch.setattr(sysobj, "_resolve", lambda values: ("h", "t", "wh", "cat"))
    monkeypatch.setattr(sysobj, "_ensure_running", lambda host, token, wh: None)
    monkeypatch.setattr(
        connection, "exec_sql",
        lambda host, token, wh, catalog, stmt, **kw: sql.append(stmt),
    )

    sysobj.create_schema({"VOLUME": "staging"}, "d_in", force=False)
    assert "CREATE SCHEMA `cat`.`d_in`" in sql
    assert "CREATE VOLUME IF NOT EXISTS `cat`.`d_in`.`staging`" in sql

    # No volume statement when the VOLUME option is unset.
    sql.clear()
    sysobj.create_schema({}, "d_in", force=False)
    assert not any("CREATE VOLUME" in s for s in sql)
