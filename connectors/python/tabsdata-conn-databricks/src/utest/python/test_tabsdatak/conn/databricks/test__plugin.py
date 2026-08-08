#
# Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the Databricks plugin -- error wrapping behaviour,
DestDef / SrcDef wiring, and the _is_connection_error classifier.

The plugin delegates the heavy lifting to ``_impl``; here we
fault-inject into the impl to verify the plugin maps every
expected exception class to the right DATABRICKS_* error code.

End-to-end coverage against a real Databricks workspace lives in the
Databricks integration suite.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.databricks import (
    DatabricksDestConn,
    DatabricksSrcConn,
    DatabricksDest,
    DatabricksSrc,
)

# noinspection PyProtectedMember
from tabsdatak.conn.databricks import _impl as impl

# noinspection PyProtectedMember
from tabsdatak.conn.databricks import _plugin as plugin
from tabsdatak.conn.common.sql.constants import (
    SQL_META_INITIAL_VALUES,
    SQL_META_QUERY,
)
from tabsdatak.conn.databricks._explorer import DatabricksConnectionExplorer
from tabsdatak.spi import TableFileInput
from tabsdatak.conn.databricks.error import (
    DatabricksErrorCode,
    DatabricksRuntimeException,
    DatabricksValidateException,
)



pytestmark = pytest.mark.databricks


# ---------------------------------------------------------------------------
# Duck-typed SPI context for unit tests
# ---------------------------------------------------------------------------


class _Ctx:
    """Duck-typed SrcPluginCtx for unit tests.

    Exposes ``work_dir`` plus a minimal in-memory scratch store so
    tests for the ctx-vs-Src-field fallback (see ``_resolve_initial_values``)
    can drive ``get_attr`` / ``set_attr`` without instantiating
    a real :class:`tabsdatak.spi.SrcPluginCtx`.
    """

    def __init__(self, work: Path, scratch: dict | None = None) -> None:
        self._work = work
        self._scratch: dict = dict(scratch) if scratch else {}

    @property
    def work_dir(self) -> Path:
        return self._work

    def get_attr(self, key):
        return self._scratch.get(key)

    def set_attr(self, key, val) -> None:
        self._scratch[key] = val


@pytest.fixture
def work_dir(tmp_path):
    out = tmp_path / "work"
    out.mkdir()
    return out


def _ctx(p: Path) -> _Ctx:
    return _Ctx(p)


# ---------------------------------------------------------------------------
# Conn / Src / Dest helpers
# ---------------------------------------------------------------------------


def _src_conn() -> DatabricksSrcConn:
    return DatabricksSrcConn(
        host_url=Secret("https://example.cloud.databricks.com"),
        token=Secret("dapi-test"),
        volume=Secret("my_volume"),
        warehouse=Secret("MY_WAREHOUSE"),
        catalog=Secret("main"),
        schema=Secret("analytics"),
    )


def _dest_conn() -> DatabricksDestConn:
    return DatabricksDestConn(
        host_url=Secret("https://example.cloud.databricks.com"),
        token=Secret("dapi-test"),
        volume=Secret("my_volume"),
        warehouse=Secret("MY_WAREHOUSE"),
        catalog=Secret("main"),
        schema=Secret("analytics"),
    )


def _src() -> DatabricksSrc:
    return DatabricksSrc(
        queries=["SELECT 1"],
    )


def _dest(
    *, tables=("main.analytics.users",), if_table_exists="append"
) -> DatabricksDest:
    return DatabricksDest(
        tables=list(tables),
        if_table_exists=if_table_exists,
    )


# ---------------------------------------------------------------------------
# Stub error classes that match the _is_connection_error classifier
# without importing the real databricks-sdk / databricks-sql modules.
# ---------------------------------------------------------------------------


class _StubDatabricksError(Exception):
    """Class-named ``DatabricksError`` so ``_is_connection_error``
    classifies it as a connect failure without importing the real
    ``databricks-sdk`` package."""


_StubDatabricksError.__name__ = "DatabricksError"


# ---------------------------------------------------------------------------
# _is_connection_error classifier
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    [
        "DatabricksError",
        "Unauthenticated",
        "PermissionDenied",
        "InterfaceError",
        "OperationalError",
        "ConnectionError",
    ],
)
def test_is_connection_error_recognises_known_names(name):
    cls = type(name, (Exception,), {})
    assert plugin._is_connection_error(cls()) is True


def test_is_connection_error_rejects_other_names():
    assert plugin._is_connection_error(RuntimeError("nope")) is False
    assert plugin._is_connection_error(ValueError("nope")) is False


# ---------------------------------------------------------------------------
# _resolve_*_cfg
# ---------------------------------------------------------------------------


def test_resolve_src_cfg_logs_unknown_keys(caplog):
    import logging as _logging

    caplog.set_level(_logging.WARNING, logger=plugin.logger.name)
    cfg = plugin._resolve_src_cfg({"databricks.logging_level": 20, "bogus": 1})
    assert cfg.logging_level == 20
    assert any("Unknown src_cfg key" in rec.getMessage() for rec in caplog.records)


def test_resolve_src_cfg_reads_drop_metadata():
    assert plugin._resolve_src_cfg(None).drop_metadata is False
    cfg = plugin._resolve_src_cfg({"tabsdata.src_metadata.drop": True})
    assert cfg.drop_metadata is True


def test_resolve_dest_cfg_defaults_when_none():
    import logging as _logging

    cfg = plugin._resolve_dest_cfg(None)
    assert cfg.logging_level == _logging.ERROR


# ---------------------------------------------------------------------------
# _DatabricksSrcPlugin.read_in -- error wrapping
# ---------------------------------------------------------------------------


def test_src_plugin_wraps_connection_failure_as_databricks_5(
    monkeypatch,
    work_dir,
):
    def boom(*_a, **_kw):
        raise _StubDatabricksError("connect fail")

    monkeypatch.setattr(impl, "read_all_queries", boom)

    with pytest.raises(DatabricksRuntimeException) as ei:
        plugin._DatabricksSrcPlugin().read_in(_ctx(work_dir), _src_conn(), _src())
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_5


def test_src_plugin_wraps_query_failure_as_databricks_6(
    monkeypatch,
    work_dir,
):
    monkeypatch.setattr(
        impl,
        "read_all_queries",
        MagicMock(side_effect=RuntimeError("query failed")),
    )
    with pytest.raises(DatabricksRuntimeException) as ei:
        plugin._DatabricksSrcPlugin().read_in(_ctx(work_dir), _src_conn(), _src())
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_6


@pytest.mark.parametrize(
    "message, expected",
    [
        pytest.param(
            "[UNBOUND_SQL_PARAMETER] Found the unbound parameter: cursor. "
            "Please, fix `args`. SQLSTATE: 42P02",
            "'cursor'",
            id="name-reported",
        ),
        pytest.param(
            "[UNBOUND_SQL_PARAMETER] Found an unbound parameter. SQLSTATE: 42P02",
            "a parameter it does not name",
            id="name-missing",
        ),
    ],
)
def test_src_plugin_wraps_unbound_bind_as_databricks_12(
    monkeypatch,
    work_dir,
    message: str,
    expected: str,
):
    monkeypatch.setattr(
        impl,
        "read_all_queries",
        MagicMock(side_effect=RuntimeError(message)),
    )
    with pytest.raises(DatabricksRuntimeException) as ei:
        plugin._DatabricksSrcPlugin().read_in(_ctx(work_dir), _src_conn(), _src())
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_12
    assert expected in str(ei.value)
    assert "initial_values" in str(ei.value)


def test_src_plugin_returns_one_parquet_per_query(
    monkeypatch,
    work_dir,
):
    def fake(_cfg, _dir):
        return [Path("q0.parquet"), Path("q1.parquet")]

    monkeypatch.setattr(impl, "read_all_queries", fake)

    src = DatabricksSrc(
        queries=["SELECT 1", "SELECT 2"],
    )
    out = plugin._DatabricksSrcPlugin().read_in(_ctx(work_dir), _src_conn(), src)
    # Each spec is a TableFileInput wrapping the adapter's Path.
    assert len(out) == 2
    assert all(len(row) == 1 for row in out)
    assert [str(out[0][0].file), str(out[1][0].file)] == [
        "q0.parquet",
        "q1.parquet",
    ]
    assert all(isinstance(out[i][0], TableFileInput) for i in (0, 1))
    assert all(isinstance(out[i][0].file, Path) for i in (0, 1))


def test_src_plugin_reports_query_and_binds_as_metadata(
    monkeypatch,
    work_dir,
):
    def fake(_cfg, _dir):
        return [Path("q0.parquet"), Path("q1.parquet")]

    monkeypatch.setattr(impl, "read_all_queries", fake)

    src = DatabricksSrc(
        queries=["SELECT * FROM t WHERE id > {min_id}", "SELECT 2"],
        initial_values={"min_id": 7},
    )
    out = plugin._DatabricksSrcPlugin().read_in(_ctx(work_dir), _src_conn(), src)
    assert [row[0].metadata[SQL_META_QUERY] for row in out] == [
        "SELECT * FROM t WHERE id > {min_id}",
        "SELECT 2",
    ]
    assert [row[0].metadata[SQL_META_INITIAL_VALUES] for row in out] == [
        '{"min_id": 7}',
        '{"min_id": 7}',
    ]


def test_src_plugin_drops_metadata_when_src_cfg_asks(monkeypatch, work_dir):
    monkeypatch.setattr(
        impl, "read_all_queries", lambda _cfg, _dir: [Path("q0.parquet")]
    )

    src = DatabricksSrc(
        queries=["SELECT 1"],
        src_cfg={"tabsdata.src_metadata.drop": True},
    )
    out = plugin._DatabricksSrcPlugin().read_in(_ctx(work_dir), _src_conn(), src)
    assert out[0][0].metadata is None


# ---------------------------------------------------------------------------
# _DatabricksDestPlugin.write_out -- error wrapping
# ---------------------------------------------------------------------------


def _parquet(work_dir: Path, name: str) -> Path:
    p = Path(work_dir / name)
    p.write_bytes(b"")  # contents irrelevant -- impl is mocked
    return p


def test_dest_plugin_raises_databricks_7_on_tables_slots_mismatch(
    work_dir,
):
    """``len(dest.tables) != len(tables)`` is caught defensively
    by the plugin without ever calling the impl."""
    dest = _dest(tables=("main.s.t1", "main.s.t2"))
    with pytest.raises(DatabricksRuntimeException) as ei:
        plugin._DatabricksDestPlugin().write_out(
            _ctx(work_dir),
            _dest_conn(),
            dest,
            [_parquet(work_dir, "in.parquet")],
        )
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_7


def test_dest_plugin_wraps_connection_failure_as_databricks_8(
    monkeypatch,
    work_dir,
):
    def boom(*_a, **_kw):
        raise _StubDatabricksError("connect fail")

    monkeypatch.setattr(impl, "write_all_tables", boom)

    with pytest.raises(DatabricksRuntimeException) as ei:
        plugin._DatabricksDestPlugin().write_out(
            _ctx(work_dir),
            _dest_conn(),
            _dest(),
            [_parquet(work_dir, "in.parquet")],
        )
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_8


def test_dest_plugin_wraps_copy_failure_as_databricks_9(
    monkeypatch,
    work_dir,
):
    monkeypatch.setattr(
        impl,
        "write_all_tables",
        MagicMock(side_effect=RuntimeError("COPY INTO failed")),
    )
    with pytest.raises(DatabricksRuntimeException) as ei:
        plugin._DatabricksDestPlugin().write_out(
            _ctx(work_dir),
            _dest_conn(),
            _dest(),
            [_parquet(work_dir, "in.parquet")],
        )
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_9


def test_dest_plugin_passes_validate_exception_through(
    monkeypatch,
    work_dir,
):
    """DatabricksValidateException (e.g. DATABRICKS_10 from
    ``_fully_qualify_table``) is already correctly typed; the
    plugin must not wrap it in DATABRICKS_9."""
    monkeypatch.setattr(
        impl,
        "write_all_tables",
        MagicMock(
            side_effect=DatabricksErrorCode.DATABRICKS_10.exception(value="bad_name"),
        ),
    )
    with pytest.raises(DatabricksValidateException) as ei:
        plugin._DatabricksDestPlugin().write_out(
            _ctx(work_dir),
            _dest_conn(),
            _dest(),
            [_parquet(work_dir, "in.parquet")],
        )
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_10


def test_dest_plugin_dispatches_to_adapter(monkeypatch, work_dir):
    captured: dict = {}

    def stub(cfg, parquet_paths):
        captured["cfg"] = cfg
        captured["parquet_paths"] = parquet_paths

    monkeypatch.setattr(impl, "write_all_tables", stub)

    src1 = _parquet(work_dir, "u.parquet")
    src2 = _parquet(work_dir, "o.parquet")
    dest = _dest(
        tables=("main.analytics.users", "main.analytics.orders"),
        if_table_exists="replace",
    )
    plugin._DatabricksDestPlugin().write_out(
        _ctx(work_dir),
        _dest_conn(),
        dest,
        [src1, src2],
    )
    assert captured["parquet_paths"] == [src1, src2]
    assert captured["cfg"].target_tables == [
        "main.analytics.users",
        "main.analytics.orders",
    ]
    assert captured["cfg"].if_table_exists == "replace"


# ---------------------------------------------------------------------------
# SrcDef / DestDef wiring
# ---------------------------------------------------------------------------


def test_databricks_src_def_wires_plugin_and_explorer():
    assert plugin.DATABRICKS_SRC.conn is DatabricksSrcConn
    assert plugin.DATABRICKS_SRC.src is DatabricksSrc
    assert plugin.DATABRICKS_SRC.plugin is plugin._DatabricksSrcPlugin
    assert plugin.DATABRICKS_SRC.explorer is DatabricksConnectionExplorer
    assert plugin.DATABRICKS_SRC.type_ == "databricks-bulk-in"


def test_databricks_dest_def_wires_plugin_and_explorer():
    assert plugin.DATABRICKS_DEST.conn is DatabricksDestConn
    assert plugin.DATABRICKS_DEST.dest is DatabricksDest
    assert plugin.DATABRICKS_DEST.plugin is plugin._DatabricksDestPlugin
    assert plugin.DATABRICKS_DEST.explorer is DatabricksConnectionExplorer
    assert plugin.DATABRICKS_DEST.type_ == "databricks-bulk-out"


def test_databricks_src_cardinality_uses_queries_length():
    src = DatabricksSrc(
        queries=["SELECT 1", "SELECT 2", "SELECT 3"],
    )
    assert plugin.DATABRICKS_SRC.cardinality(src) == 3


def test_databricks_dest_cardinality_uses_tables_length():
    dest = _dest(tables=("main.s.t1", "main.s.t2", "main.s.t3"))
    assert plugin.DATABRICKS_DEST.cardinality(dest) == 3


# ---------------------------------------------------------------------------
# _resolve_initial_values -- per-bind-name ctx lookup with Src field fallback
# ---------------------------------------------------------------------------


def test_resolve_initial_values_returns_none_when_src_has_no_seed(work_dir):
    """No Src seed → ``None``. Connector skips substitution entirely
    (no placeholders, no work to do)."""
    ctx = _Ctx(work_dir)
    src = _src()  # initial_values defaults to None
    assert plugin._resolve_initial_values(ctx, src) is None


def test_resolve_initial_values_returns_seed_when_ctx_empty(work_dir):
    """First run for a flow: ctx scratch has no entry for any bind
    name, fall back to the user-supplied Src seed for each."""
    ctx = _Ctx(work_dir)
    src = DatabricksSrc(
        queries=["SELECT 1"],
        initial_values={"last_modified": "2026-01-01T00:00:00Z", "min_id": 0},
    )
    out = plugin._resolve_initial_values(ctx, src)
    assert out == {"last_modified": "2026-01-01T00:00:00Z", "min_id": 0}


def test_resolve_initial_values_prefers_ctx_per_bind_name(work_dir):
    """Subsequent run: each bind name independently picks up its
    most recent value via ``ctx.get_attr(name)``. Seed is only
    used for the names ctx doesn't have."""
    ctx = _Ctx(
        work_dir,
        scratch={
            "last_modified": "2026-05-28T00:00:00Z",
            "min_id": 1234,
        },
    )
    src = DatabricksSrc(
        queries=["SELECT 1"],
        initial_values={"last_modified": "from-seed", "min_id": 0},
    )
    out = plugin._resolve_initial_values(ctx, src)
    assert out == {
        "last_modified": "2026-05-28T00:00:00Z",
        "min_id": 1234,
    }


def test_resolve_initial_values_partial_ctx_falls_back_per_name(work_dir):
    """If ctx has one bind name set but not another, the missing
    name falls back to the seed -- per-name granularity."""
    ctx = _Ctx(work_dir, scratch={"last_modified": "2026-05-28T00:00:00Z"})
    src = DatabricksSrc(
        queries=["SELECT 1"],
        initial_values={"last_modified": "seed-ts", "min_id": 42},
    )
    out = plugin._resolve_initial_values(ctx, src)
    assert out == {"last_modified": "2026-05-28T00:00:00Z", "min_id": 42}


def test_resolve_initial_values_preserves_falsy_ctx_values(work_dir):
    """Uses ``is not None`` (not truthiness): a legitimately falsy
    value persisted last run (``0``, ``False``, ``""``) isn't
    silently shadowed by the seed."""
    ctx = _Ctx(work_dir, scratch={"min_id": 0, "flag": False, "label": ""})
    src = DatabricksSrc(
        queries=["SELECT 1"],
        initial_values={"min_id": 999, "flag": True, "label": "default"},
    )
    out = plugin._resolve_initial_values(ctx, src)
    assert out == {"min_id": 0, "flag": False, "label": ""}


def test_resolve_initial_values_heterogeneous_primitive_values(work_dir):
    """The realistic SQL use case: bind values of mixed primitive
    types (str + int). The new flat-key model round-trips them
    cleanly."""
    ctx = _Ctx(
        work_dir,
        scratch={
            "last_modified": "2026-01-15T00:00:00Z",
            "min_id": 1234,
        },
    )
    src = DatabricksSrc(
        queries=["SELECT 1"],
        initial_values={"last_modified": "seed", "min_id": 0},
    )
    out = plugin._resolve_initial_values(ctx, src)
    assert isinstance(out["last_modified"], str)
    assert isinstance(out["min_id"], int)
    assert out == {"last_modified": "2026-01-15T00:00:00Z", "min_id": 1234}


def test_read_in_uses_resolved_initial_values(work_dir, monkeypatch):
    """``read_in`` must pass the *resolved* dict (ctx-first per
    bind name, Src fallback) into ``DatabricksReadCfg``, not the
    raw Src field."""
    captured: dict = {}

    def fake_read_all_queries(read_cfg, work_dir_arg):
        captured["initial_values"] = read_cfg.initial_values
        return [Path("q0.parquet")]

    monkeypatch.setattr(impl, "read_all_queries", fake_read_all_queries)

    ctx = _Ctx(work_dir, scratch={"last_modified": "from-ctx"})
    src = DatabricksSrc(
        queries=["SELECT * FROM t WHERE ts > {last_modified}"],
        initial_values={"last_modified": "from-seed-ignored"},
    )
    plugin._DatabricksSrcPlugin().read_in(ctx, _src_conn(), src)
    assert captured["initial_values"] == {"last_modified": "from-ctx"}
