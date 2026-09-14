#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from test_tabsdatak.conn.snowflake.support import _FakeCtx

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.snowflake import MergeCfg, SnowflakeDestConn, SnowflakeDest

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake import _impl as impl

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake import _plugin as plugin
from tabsdatak.conn.snowflake.error import (
    SnowflakeErrorCode,
    SnowflakeRuntimeException,
    SnowflakeValidateException,
)
from tabsdatak.conn.snowflake._explorer import SnowflakeConnectionExplorer

pytestmark = pytest.mark.snowflake


# ---------------------------------------------------------------------------
# Test context shims for DestContext
# ---------------------------------------------------------------------------


def _dest_ctx(work: Path) -> _FakeCtx:
    return _FakeCtx(work=work)


def _credentials() -> UserPassword:
    return UserPassword(
        user=Secret("etl"),
        password=Secret("hunter2"),
    )


def _conn(**overrides) -> SnowflakeDestConn:
    kwargs: dict = dict(
        account="ACME-MAIN",
        credentials=_credentials(),
        role="ETL_ROLE",
        database="ANALYTICS",
        schema="PUBLIC",
        warehouse="ETL_WH",
        stage="MY_STAGE",
    )
    kwargs.update(overrides)
    return SnowflakeDestConn(**kwargs)


def _bare_conn() -> SnowflakeDestConn:
    """Minimal SnowflakeDestConn for error tests."""
    return SnowflakeDestConn(
        account="ACME-MAIN",
        credentials=_credentials(),
    )


def _make_parquet(path: Path, rows: list[dict]) -> Path:
    pl.DataFrame(rows).write_parquet(path, use_pyarrow=True)
    return path


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    out = tmp_path / "work"
    out.mkdir()
    return out


# ---------------------------------------------------------------------------
# Error-class stand-ins (the snowflake.connector driver may not be
# installed; we shadow its exception classes by name only so the
# plugin's _is_connection_error classifier matches them).
# ---------------------------------------------------------------------------


class DatabaseError(Exception):
    """Stand-in for ``snowflake.connector.errors.DatabaseError``."""


class ProgrammingError(Exception):
    """Stand-in for ``snowflake.connector.errors.ProgrammingError``."""


# ---------------------------------------------------------------------------
# SnowflakeDestPlugin.write_out -- happy path
#
# Mock ``_impl.write_all_tables`` directly so the plugin's contract
# with the impl layer is the thing under test (target_tables /
# if_table_exists / files forwarding).
# ---------------------------------------------------------------------------


def test_dest_plugin_defaults_to_the_native_engine(work_dir):
    cfg = plugin._resolve_dest_cfg(None)
    assert cfg.schema_evolution_engine == "native"


@pytest.mark.parametrize("engine", ["native", "iceberg"])
def test_dest_plugin_reads_the_engine_off_dest_cfg(engine):
    cfg = plugin._resolve_dest_cfg({"snowflake.schema_evolution_engine": engine})
    assert cfg.schema_evolution_engine == engine


def test_dest_plugin_rejects_an_unknown_engine():
    """A typo would otherwise fall back to native evolution and lose the
    widening the write asked for, with nothing said about it."""
    with pytest.raises(SnowflakeValidateException) as ei:
        plugin._resolve_dest_cfg({"snowflake.schema_evolution_engine": "icebrg"})
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_21
    assert "'icebrg'" in str(ei.value)


def test_dest_plugin_forwards_the_engine_to_the_write_cfg(monkeypatch, work_dir):
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    captured: dict = {}
    monkeypatch.setattr(
        impl, "write_all_tables", lambda cfg, tables: captured.update(cfg=cfg)
    )
    dest = SnowflakeDest(
        tables=["ANALYTICS.PUBLIC.USERS"],
        dest_cfg={"snowflake.schema_evolution_engine": "iceberg"},
    )
    plugin.SnowflakeDestPlugin().write_out(_dest_ctx(work_dir), _conn(), dest, [src])
    assert captured["cfg"].schema_evolution_engine == "iceberg"


def test_dest_plugin_write_out_calls_write_all_tables(monkeypatch, work_dir):
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])

    captured: dict = {}

    def fake_write_all_tables(cfg, tables):
        captured["cfg"] = cfg
        captured["tables"] = list(tables)

    monkeypatch.setattr(impl, "write_all_tables", fake_write_all_tables)

    dest = SnowflakeDest(
        tables=["ANALYTICS.PUBLIC.USERS"],
        if_table_exists="replace",
        dest_cfg={"snowflake.logging_level": 20},
    )
    plugin.SnowflakeDestPlugin().write_out(_dest_ctx(work_dir), _conn(), dest, [src])

    cfg = captured["cfg"]
    assert isinstance(cfg, impl.SnowflakeWriteCfg)
    assert cfg.target_tables == ["ANALYTICS.PUBLIC.USERS"]
    assert cfg.if_table_exists == "replace"
    assert cfg.schema_evolution == "strict"
    assert cfg.schema_evolution_engine == "native"
    assert cfg.merge_cfgs is None
    assert captured["tables"] == [src]


def test_dest_plugin_forwards_one_merge_cfg_per_table(monkeypatch, work_dir):
    """The configs reach the impl one per table, in the destination's order, so
    the write side can index them by slot without re-deriving the mapping."""
    files = [
        _make_parquet(work_dir / "a.parquet", [{"id": 1}]),
        _make_parquet(work_dir / "b.parquet", [{"id": 2}]),
    ]
    captured: dict = {}
    monkeypatch.setattr(
        impl, "write_all_tables", lambda cfg, tables: captured.update(cfg=cfg)
    )

    first = MergeCfg(on=["id"], op_column="op")
    second = MergeCfg(on=["other_id"], op_column="op")
    dest = SnowflakeDest(
        tables=["DB.S.A", "DB.S.B"],
        if_table_exists="merge",
        merge_cfg=[first, second],
        dest_cfg={"snowflake.schema_evolution_engine": "iceberg"},
    )
    plugin.SnowflakeDestPlugin().write_out(_dest_ctx(work_dir), _conn(), dest, files)

    assert captured["cfg"].merge_cfgs == [first, second]


@pytest.mark.parametrize("schema_evolution", ["strict", "update"])
def test_dest_plugin_forwards_schema_evolution(
    monkeypatch, work_dir, schema_evolution
):
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    captured: dict = {}
    monkeypatch.setattr(
        impl, "write_all_tables", lambda cfg, tables: captured.update(cfg=cfg)
    )

    dest = SnowflakeDest(
        tables=["ANALYTICS.PUBLIC.USERS"], schema_evolution=schema_evolution
    )
    plugin.SnowflakeDestPlugin().write_out(_dest_ctx(work_dir), _conn(), dest, [src])

    assert captured["cfg"].schema_evolution == schema_evolution


# ---------------------------------------------------------------------------
# SnowflakeDestPlugin.write_out -- mismatch error (SNOWFLAKE_4)
# ---------------------------------------------------------------------------


def test_dest_plugin_raises_snowflake_4_on_tables_slots_mismatch(work_dir):
    dest = SnowflakeDest(
        tables=["USERS", "ORDERS"],
    )
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(SnowflakeRuntimeException) as ei:
        plugin.SnowflakeDestPlugin().write_out(
            _dest_ctx(work_dir),
            _bare_conn(),
            dest,
            [src],
        )
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_4


# ---------------------------------------------------------------------------
# SnowflakeDestPlugin.write_out -- error wrapping (fault injection)
#
# Inject failures into ``_impl.write_all_tables`` and verify the
# plugin maps them onto the right SNOWFLAKE_ error code.
# ---------------------------------------------------------------------------


def test_dest_plugin_wraps_database_error_as_snowflake_5(monkeypatch, work_dir):
    def boom(cfg, tables):
        raise DatabaseError("conn fail")

    monkeypatch.setattr(impl, "write_all_tables", boom)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = SnowflakeDest(tables=["USERS"])
    with pytest.raises(SnowflakeRuntimeException) as ei:
        plugin.SnowflakeDestPlugin().write_out(
            _dest_ctx(work_dir), _conn(), dest, [src]
        )
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_5


def test_dest_plugin_wraps_programming_error_as_snowflake_6(monkeypatch, work_dir):
    def boom(cfg, tables):
        raise ProgrammingError("bad sql")

    monkeypatch.setattr(impl, "write_all_tables", boom)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = SnowflakeDest(tables=["USERS"])
    with pytest.raises(SnowflakeRuntimeException) as ei:
        plugin.SnowflakeDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_conn(), dest, [src]
        )
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_6


def test_dest_plugin_wraps_error_mentions_account(monkeypatch, work_dir):
    """The SNOWFLAKE_5 message embeds the account."""

    def boom(cfg, tables):
        raise DatabaseError("x")

    monkeypatch.setattr(impl, "write_all_tables", boom)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = SnowflakeDest(tables=["USERS"])
    with pytest.raises(SnowflakeRuntimeException) as ei:
        plugin.SnowflakeDestPlugin().write_out(
            _dest_ctx(work_dir), _conn(), dest, [src]
        )
    assert "ACME-MAIN" in str(ei.value)


def test_dest_plugin_wraps_error_mentions_tables(monkeypatch, work_dir):
    """The SNOWFLAKE_6 message embeds the destination tables."""

    def boom(cfg, tables):
        raise ProgrammingError("x")

    monkeypatch.setattr(impl, "write_all_tables", boom)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = SnowflakeDest(tables=["USERS"])
    with pytest.raises(SnowflakeRuntimeException) as ei:
        plugin.SnowflakeDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_conn(), dest, [src]
        )
    assert "USERS" in str(ei.value)


# ---------------------------------------------------------------------------
# DestDef wiring
# ---------------------------------------------------------------------------


def test_snowflake_dest_def_wires_plugin_and_explorer():
    assert plugin.SNOWFLAKE_DEST.conn is SnowflakeDestConn
    assert plugin.SNOWFLAKE_DEST.dest is SnowflakeDest
    assert plugin.SNOWFLAKE_DEST.plugin is plugin.SnowflakeDestPlugin
    assert plugin.SNOWFLAKE_DEST.explorer is SnowflakeConnectionExplorer
    assert plugin.SNOWFLAKE_DEST.type_ == "snowflake-bulk-out"
    assert plugin.SNOWFLAKE_DEST.dest_version == "v1"


def test_snowflake_dest_cardinality_uses_tables_length():
    dest = SnowflakeDest(
        tables=["USERS", "ORDERS", "EVENTS"],
    )
    assert plugin.SNOWFLAKE_DEST.cardinality(dest) == 3


# ---------------------------------------------------------------------------
# _resolve_dest_cfg
# ---------------------------------------------------------------------------


def test_resolve_dest_cfg_returns_defaults_when_empty():
    import logging as _logging

    cfg = plugin._resolve_dest_cfg(None)
    assert cfg.logging_level == _logging.ERROR


def test_resolve_dest_cfg_applies_overrides():
    cfg = plugin._resolve_dest_cfg({"snowflake.logging_level": 20})
    assert cfg.logging_level == 20


def test_resolve_dest_cfg_warns_on_unknown_key_via_raw_dict(caplog):
    with caplog.at_level("WARNING", logger=plugin.logger.name):
        plugin._resolve_dest_cfg({"bogus.key": "value"})
    assert any("Unknown dest_cfg key" in r.getMessage() for r in caplog.records)
