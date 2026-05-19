from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.snowflake import SnowflakeConn, SnowflakeDest

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake import _impl as impl

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake import _plugin as plugin
from tabsdatak.conn.snowflake.error import (
    SnowflakeErrorCode,
    SnowflakeRuntimeException,
)


pytestmark = pytest.mark.snowflake


# ---------------------------------------------------------------------------
# Test context shims for DestContext
# ---------------------------------------------------------------------------


@dataclass
class _Ctx:
    _work: Path

    def work_dir(self) -> Path:
        return self._work


def _dest_ctx(work: Path) -> _Ctx:
    return _Ctx(work)


def _credentials() -> UserPassword:
    return UserPassword(
        user=Secret.lit("etl"),
        password=Secret.lit("hunter2"),
    )


def _conn(**overrides) -> SnowflakeConn:
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
    return SnowflakeConn(**kwargs)


def _bare_conn() -> SnowflakeConn:
    """Minimal SnowflakeConn for error tests."""
    return SnowflakeConn(
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


def test_dest_plugin_write_out_calls_write_all_tables(monkeypatch, work_dir):
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])

    captured: dict = {}

    def fake_write_all_tables(cfg, tables):
        captured["cfg"] = cfg
        captured["tables"] = list(tables)

    monkeypatch.setattr(impl, "write_all_tables", fake_write_all_tables)

    dest = SnowflakeDest(
        conn=_conn(),
        tables=["ANALYTICS.PUBLIC.USERS"],
        if_table_exists="replace",
        dest_cfg={"snowflake.logging_level": 20},
    )
    plugin.SnowflakeDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])

    cfg = captured["cfg"]
    assert isinstance(cfg, impl.SnowflakeWriteCfg)
    assert cfg.target_tables == ["ANALYTICS.PUBLIC.USERS"]
    assert cfg.if_table_exists == "replace"
    assert captured["tables"] == [src]


# ---------------------------------------------------------------------------
# SnowflakeDestPlugin.write_out -- mismatch error (SNOWFLAKE_4)
# ---------------------------------------------------------------------------


def test_dest_plugin_raises_snowflake_4_on_tables_slots_mismatch(work_dir):
    dest = SnowflakeDest(
        conn=_bare_conn(),
        tables=["USERS", "ORDERS"],
    )
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(SnowflakeRuntimeException) as ei:
        plugin.SnowflakeDestPlugin().write_out(
            _dest_ctx(work_dir), dest, [src],
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
    dest = SnowflakeDest(conn=_conn(), tables=["USERS"])
    with pytest.raises(SnowflakeRuntimeException) as ei:
        plugin.SnowflakeDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_5


def test_dest_plugin_wraps_programming_error_as_snowflake_6(monkeypatch, work_dir):
    def boom(cfg, tables):
        raise ProgrammingError("bad sql")

    monkeypatch.setattr(impl, "write_all_tables", boom)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = SnowflakeDest(conn=_bare_conn(), tables=["USERS"])
    with pytest.raises(SnowflakeRuntimeException) as ei:
        plugin.SnowflakeDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_6


def test_dest_plugin_wraps_error_mentions_account(monkeypatch, work_dir):
    """The SNOWFLAKE_5 message embeds the account."""
    def boom(cfg, tables):
        raise DatabaseError("x")

    monkeypatch.setattr(impl, "write_all_tables", boom)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = SnowflakeDest(conn=_conn(), tables=["USERS"])
    with pytest.raises(SnowflakeRuntimeException) as ei:
        plugin.SnowflakeDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    assert "ACME-MAIN" in str(ei.value)


def test_dest_plugin_wraps_error_mentions_tables(monkeypatch, work_dir):
    """The SNOWFLAKE_6 message embeds the destination tables."""
    def boom(cfg, tables):
        raise ProgrammingError("x")

    monkeypatch.setattr(impl, "write_all_tables", boom)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = SnowflakeDest(conn=_bare_conn(), tables=["USERS"])
    with pytest.raises(SnowflakeRuntimeException) as ei:
        plugin.SnowflakeDestPlugin().write_out(_dest_ctx(work_dir), dest, [src])
    assert "USERS" in str(ei.value)


# ---------------------------------------------------------------------------
# Explorer stubs
# ---------------------------------------------------------------------------


def test_explorer_methods_are_stubs():
    exp = plugin.SnowflakeExplorer()
    assert exp.metadata(_bare_conn()) is None
    assert exp.data(_bare_conn(), "SELECT 1") is None


# ---------------------------------------------------------------------------
# DestDef wiring
# ---------------------------------------------------------------------------


def test_snowflake_dest_def_wires_plugin_and_explorer():
    assert plugin.SNOWFLAKE_DEST.conn is SnowflakeConn
    assert plugin.SNOWFLAKE_DEST.dest is SnowflakeDest
    assert plugin.SNOWFLAKE_DEST.plugin is plugin.SnowflakeDestPlugin
    assert plugin.SNOWFLAKE_DEST.explorer is plugin.SnowflakeExplorer
    assert plugin.SNOWFLAKE_DEST.dest_type == "snowflake"
    assert plugin.SNOWFLAKE_DEST.dest_version == "v1"


def test_snowflake_dest_cardinality_uses_tables_length():
    dest = SnowflakeDest(
        conn=_bare_conn(),
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
    assert any("Unknown dest_cfg key" in r.message for r in caplog.records)
