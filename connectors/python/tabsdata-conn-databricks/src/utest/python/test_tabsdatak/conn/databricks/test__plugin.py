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

from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql.constants import (
    SQL_META_INITIAL_VALUES,
    SQL_META_QUERY,
)

# noinspection PyProtectedMember
from tabsdatak.conn.databricks import (
    DatabricksDest,
    DatabricksDestConn,
    DatabricksSrc,
    DatabricksSrcConn,
    _impl as impl,
    _plugin as plugin,
)
from tabsdatak.conn.databricks._explorer import DatabricksConnectionExplorer
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

    Exposes ``work_dir`` plus a minimal in-memory attribute store so
    tests for the ctx-vs-Src-field fallback (see ``resolve_initial_values``)
    can drive ``get_attr`` / ``set_attr`` without instantiating
    a real :class:`tabsdatak.spi.SrcPluginCtx`.
    """

    def __init__(
        self, work: Path, attributes: dict | None = None, transaction_id: str = "TRX_1"
    ) -> None:
        self._work = work
        self._attributes: dict = dict(attributes) if attributes else {}
        self.transaction_id = transaction_id

    @property
    def work_dir(self) -> Path:
        return self._work

    def get_attr(self, key):
        return self._attributes.get(key)

    def set_attr(self, key, val) -> None:
        self._attributes[key] = val


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


def _failing(error: Exception):
    def tables():
        raise error
        yield

    return lambda _cfg: [tables()]


def _drained(out) -> list[list]:
    return [list(slot) for slot in out]


def test_src_plugin_reads_nothing_until_iterated(monkeypatch, work_dir):
    monkeypatch.setattr(impl, "read_all_queries", _failing(RuntimeError("late")))
    out = plugin._DatabricksSrcPlugin().read_in(_ctx(work_dir), _src_conn(), _src())
    assert len(out) == 1


def test_src_plugin_wraps_connection_failure_as_databricks_5(
    monkeypatch,
    work_dir,
):
    monkeypatch.setattr(
        impl, "read_all_queries", _failing(_StubDatabricksError("connect fail"))
    )
    out = plugin._DatabricksSrcPlugin().read_in(_ctx(work_dir), _src_conn(), _src())
    with pytest.raises(DatabricksRuntimeException) as ei:
        _drained(out)
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_5


def test_src_plugin_wraps_query_failure_as_databricks_6(
    monkeypatch,
    work_dir,
):
    monkeypatch.setattr(impl, "read_all_queries", _failing(RuntimeError("failed")))
    out = plugin._DatabricksSrcPlugin().read_in(_ctx(work_dir), _src_conn(), _src())
    with pytest.raises(DatabricksRuntimeException) as ei:
        _drained(out)
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_6
    assert "query_index=0" in str(ei.value)


def test_src_plugin_passes_its_own_errors_through(monkeypatch, work_dir):
    monkeypatch.setattr(
        impl,
        "read_all_queries",
        _failing(DatabricksErrorCode.DATABRICKS_11.exception(query_index=0)),
    )
    out = plugin._DatabricksSrcPlugin().read_in(_ctx(work_dir), _src_conn(), _src())
    with pytest.raises(DatabricksRuntimeException) as ei:
        _drained(out)
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_11


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
    monkeypatch.setattr(impl, "read_all_queries", _failing(RuntimeError(message)))
    out = plugin._DatabricksSrcPlugin().read_in(_ctx(work_dir), _src_conn(), _src())
    with pytest.raises(DatabricksRuntimeException) as ei:
        _drained(out)
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_12
    assert expected in str(ei.value)
    assert "initial_values" in str(ei.value)


def _results(*queries: list[pa.Table]):
    return lambda _cfg: [iter(tables) for tables in queries]


def test_src_plugin_hands_over_a_frame_per_chunk(monkeypatch, work_dir):
    monkeypatch.setattr(
        impl,
        "read_all_queries",
        _results(
            [pa.table({"id": [1, 2]}), pa.table({"id": [3]})],
            [pa.table({"name": ["a"]})],
        ),
    )
    src = DatabricksSrc(queries=["SELECT 1", "SELECT 2"])
    out = plugin._DatabricksSrcPlugin().read_in(_ctx(work_dir), _src_conn(), src)
    frames = [[table.frame.collect().to_dicts() for table in slot] for slot in out]
    assert frames == [
        [[{"id": 1}, {"id": 2}], [{"id": 3}]],
        [[{"name": "a"}]],
    ]


def test_src_plugin_reports_query_and_binds_as_metadata(
    monkeypatch,
    work_dir,
):
    monkeypatch.setattr(
        impl,
        "read_all_queries",
        _results([pa.table({"id": [1]})], [pa.table({"id": [2]})]),
    )
    src = DatabricksSrc(
        queries=["SELECT * FROM t WHERE id > {min_id}", "SELECT 2"],
        initial_values={"min_id": 7},
    )
    out = plugin._DatabricksSrcPlugin().read_in(_ctx(work_dir), _src_conn(), src)
    columns = [[table.metadata.columns for table in slot] for slot in out]
    assert [slot[0][SQL_META_QUERY] for slot in columns] == [
        "SELECT * FROM t WHERE id > {min_id}",
        "SELECT 2",
    ]
    assert [slot[0][SQL_META_INITIAL_VALUES] for slot in columns] == [
        '{"min_id": 7}',
        '{"min_id": 7}',
    ]


def test_src_plugin_drops_metadata_when_src_cfg_asks(monkeypatch, work_dir):
    monkeypatch.setattr(impl, "read_all_queries", _results([pa.table({"id": [1]})]))
    src = DatabricksSrc(
        queries=["SELECT 1"],
        src_cfg={"tabsdata.src_metadata.drop": True},
    )
    out = plugin._DatabricksSrcPlugin().read_in(_ctx(work_dir), _src_conn(), src)
    assert [table.metadata for table in out[0]] == [None]


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
# read_in resolves the initial values -- per-bind-name ctx lookup with Src
# field fallback
# ---------------------------------------------------------------------------


def test_read_in_uses_resolved_initial_values(work_dir, monkeypatch):
    """``read_in`` must pass the *resolved* dict (ctx-first per
    bind name, Src fallback) into ``DatabricksReadCfg``, not the
    raw Src field."""
    captured: dict = {}

    def fake_read_all_queries(read_cfg):
        captured["initial_values"] = read_cfg.initial_values
        return [iter([])]

    monkeypatch.setattr(impl, "read_all_queries", fake_read_all_queries)

    ctx = _Ctx(work_dir, attributes={"last_modified": "from-ctx"})
    src = DatabricksSrc(
        queries=["SELECT * FROM t WHERE ts > {last_modified}"],
        initial_values={"last_modified": "from-seed-ignored"},
    )
    plugin._DatabricksSrcPlugin().read_in(ctx, _src_conn(), src)
    assert captured["initial_values"] == {"last_modified": "from-ctx"}
