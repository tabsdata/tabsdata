"""Unit tests for the Databricks plugin -- error wrapping behaviour,
DestDef / SrcDef wiring, and the _is_connection_error classifier.

The plugin delegates the heavy lifting to ``_impl``; here we
fault-inject into the impl to verify the plugin maps every
expected exception class to the right DATABRICKS_* error code.

End-to-end coverage against a real Databricks workspace lives in
``test__plugin_integration.py``.
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
from tabsdatak.conn.databricks.error import (
    DatabricksErrorCode,
    DatabricksRuntimeException,
    DatabricksValidateException,
)
from tabsdatak.spi import ParquetFile


pytestmark = pytest.mark.databricks


# ---------------------------------------------------------------------------
# Duck-typed SPI context for unit tests
# ---------------------------------------------------------------------------


class _Ctx:
    """Duck-typed SrcPluginCtx for unit tests.

    Exposes ``work_dir`` plus a minimal in-memory scratch store so
    tests for the ctx-vs-Src-field fallback (see ``_resolve_initial_values``)
    can drive ``get_attr`` / ``store_attr`` without instantiating
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

    def store_attr(self, key, val) -> None:
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
        host_url=Secret.lit("https://example.cloud.databricks.com"),
        token=Secret.lit("dapi-test"),
        volume=Secret.lit("my_volume"),
        warehouse=Secret.lit("MY_WAREHOUSE"),
        catalog=Secret.lit("main"),
        schema=Secret.lit("analytics"),
    )


def _dest_conn() -> DatabricksDestConn:
    return DatabricksDestConn(
        host_url=Secret.lit("https://example.cloud.databricks.com"),
        token=Secret.lit("dapi-test"),
        volume=Secret.lit("my_volume"),
        warehouse=Secret.lit("MY_WAREHOUSE"),
        catalog=Secret.lit("main"),
        schema=Secret.lit("analytics"),
    )


def _src() -> DatabricksSrc:
    return DatabricksSrc(
        conn=_src_conn(),
        queries=["SELECT 1"],
    )


def _dest(
    *, tables=("main.analytics.users",), if_table_exists="append"
) -> DatabricksDest:
    return DatabricksDest(
        conn=_dest_conn(),
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
        plugin._DatabricksSrcPlugin().read_in(_ctx(work_dir), _src())
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
        plugin._DatabricksSrcPlugin().read_in(_ctx(work_dir), _src())
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_6


def test_src_plugin_returns_one_parquet_per_query(
    monkeypatch,
    work_dir,
):
    def fake(_cfg, _dir):
        return [Path("q0.parquet"), Path("q1.parquet")]

    monkeypatch.setattr(impl, "read_all_queries", fake)

    src = DatabricksSrc(
        conn=_src_conn(),
        queries=["SELECT 1", "SELECT 2"],
    )
    out = plugin._DatabricksSrcPlugin().read_in(_ctx(work_dir), src)
    # Each spec IS a Path (ParquetFile subclass).
    assert len(out) == 2
    assert all(len(row) == 1 for row in out)
    assert [str(out[0][0]), str(out[1][0])] == [
        "q0.parquet",
        "q1.parquet",
    ]
    assert all(isinstance(out[i][0], ParquetFile) for i in (0, 1))


# ---------------------------------------------------------------------------
# _DatabricksDestPlugin.write_out -- error wrapping
# ---------------------------------------------------------------------------


def _parquet(work_dir: Path, name: str) -> ParquetFile:
    p = ParquetFile(work_dir / name)
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
# Explorer stubs
# ---------------------------------------------------------------------------


def test_explorer_methods_are_stubs():
    src_exp = plugin._DatabricksSrcExplorer()
    assert src_exp.metadata(_src_conn()) is None
    assert src_exp.data(_src_conn(), "SELECT 1") is None
    dest_exp = plugin._DatabricksDestExplorer()
    assert dest_exp.metadata(_dest_conn()) is None
    assert dest_exp.data(_dest_conn(), "SELECT 1") is None


# ---------------------------------------------------------------------------
# SrcDef / DestDef wiring
# ---------------------------------------------------------------------------


def test_databricks_src_def_wires_plugin_and_explorer():
    assert plugin.DATABRICKS_SRC.conn is DatabricksSrcConn
    assert plugin.DATABRICKS_SRC.src is DatabricksSrc
    assert plugin.DATABRICKS_SRC.plugin is plugin._DatabricksSrcPlugin
    assert plugin.DATABRICKS_SRC.explorer is plugin._DatabricksSrcExplorer
    assert plugin.DATABRICKS_SRC.src_type == "databricks"


def test_databricks_dest_def_wires_plugin_and_explorer():
    assert plugin.DATABRICKS_DEST.conn is DatabricksDestConn
    assert plugin.DATABRICKS_DEST.dest is DatabricksDest
    assert plugin.DATABRICKS_DEST.plugin is plugin._DatabricksDestPlugin
    assert plugin.DATABRICKS_DEST.explorer is plugin._DatabricksDestExplorer
    assert plugin.DATABRICKS_DEST.dest_type == "databricks"


def test_databricks_src_cardinality_uses_queries_length():
    src = DatabricksSrc(
        conn=_src_conn(),
        queries=["SELECT 1", "SELECT 2", "SELECT 3"],
    )
    assert plugin.DATABRICKS_SRC.cardinality(src) == 3


def test_databricks_dest_cardinality_uses_tables_length():
    dest = _dest(tables=("main.s.t1", "main.s.t2", "main.s.t3"))
    assert plugin.DATABRICKS_DEST.cardinality(dest) == 3


# ---------------------------------------------------------------------------
# _resolve_initial_values -- ctx-first with Src field fallback
# ---------------------------------------------------------------------------


from tabsdatak.conn.databricks import DATABRICKS_INITIAL_VALUES_KEY


def test_resolve_initial_values_returns_none_when_both_empty(work_dir):
    """No ctx value, no Src seed → ``None``. Connector skips
    substitution entirely."""
    ctx = _Ctx(work_dir)
    src = _src()  # initial_values defaults to None
    assert plugin._resolve_initial_values(ctx, src) is None


def test_resolve_initial_values_returns_src_when_ctx_empty(work_dir):
    """First run for a flow: ctx scratch has no entry, fall back to
    the user-supplied Src seed."""
    ctx = _Ctx(work_dir)
    src = DatabricksSrc(
        conn=_src_conn(),
        queries=["SELECT 1"],
        initial_values={"last-modified": "2026-01-01T00:00:00Z"},
    )
    out = plugin._resolve_initial_values(ctx, src)
    assert out == {"last-modified": "2026-01-01T00:00:00Z"}


def test_resolve_initial_values_returns_ctx_when_present(work_dir):
    """Subsequent run: ctx scratch has a value (from previous run's
    drain). The Src seed is ignored even if non-empty."""
    ctx = _Ctx(
        work_dir,
        scratch={
            DATABRICKS_INITIAL_VALUES_KEY: {"last-modified": "2026-05-28T00:00:00Z"}
        },
    )
    src = DatabricksSrc(
        conn=_src_conn(),
        queries=["SELECT 1"],
        initial_values={"last-modified": "from-seed-should-be-ignored"},
    )
    out = plugin._resolve_initial_values(ctx, src)
    assert out == {"last-modified": "2026-05-28T00:00:00Z"}


def test_resolve_initial_values_returns_empty_dict_when_ctx_has_empty_dict(
    work_dir,
):
    """``is not None`` (not truthiness) -- a legitimately empty dict
    stored last run isn't silently shadowed by the Src seed."""
    ctx = _Ctx(work_dir, scratch={DATABRICKS_INITIAL_VALUES_KEY: {}})
    src = DatabricksSrc(
        conn=_src_conn(),
        queries=["SELECT 1"],
        initial_values={"k": "v"},
    )
    out = plugin._resolve_initial_values(ctx, src)
    assert out == {}


def test_resolve_initial_values_returns_copy_not_alias(work_dir):
    """The returned dict must be a copy so the caller mutating it
    can't bleed into the ctx scratch / Src field."""
    src_seed = {"k": "v"}
    src = DatabricksSrc(
        conn=_src_conn(),
        queries=["SELECT 1"],
        initial_values=src_seed,
    )
    ctx = _Ctx(work_dir)
    out = plugin._resolve_initial_values(ctx, src)
    assert out is not src_seed
    out["k"] = "tampered"
    assert src.initial_values == {"k": "v"}


def test_resolve_initial_values_ctx_non_dict_value_raises(work_dir):
    """If a (misbehaving) user code path stores a non-dict at the
    connector's key, we must surface the contract violation
    explicitly. Silent fall-through would either run the queries
    without substitution (wrong data) or hit a literal ``{key}``
    placeholder in SQL (confusing syntax error from Databricks)."""
    ctx = _Ctx(work_dir, scratch={DATABRICKS_INITIAL_VALUES_KEY: "not-a-dict"})
    src = DatabricksSrc(
        conn=_src_conn(),
        queries=["SELECT 1"],
        initial_values={"k": "from-seed"},
    )
    with pytest.raises(DatabricksValidateException) as ei:
        plugin._resolve_initial_values(ctx, src)
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_11
    assert "str" in str(ei.value)


def test_read_in_uses_resolved_initial_values(work_dir, monkeypatch):
    """``read_in`` must pass the *resolved* dict (ctx-first, Src
    fallback) into ``DatabricksReadCfg``, not the raw Src field."""
    captured: dict = {}

    def fake_read_all_queries(read_cfg, work_dir_arg):
        captured["initial_values"] = read_cfg.initial_values
        return []

    monkeypatch.setattr(impl, "read_all_queries", fake_read_all_queries)

    ctx = _Ctx(
        work_dir,
        scratch={DATABRICKS_INITIAL_VALUES_KEY: {"last-modified": "from-ctx"}},
    )
    src = DatabricksSrc(
        conn=_src_conn(),
        queries=["SELECT * FROM t WHERE ts > {last-modified}"],
        initial_values={"last-modified": "from-seed-ignored"},
    )
    plugin._DatabricksSrcPlugin().read_in(ctx, src)
    assert captured["initial_values"] == {"last-modified": "from-ctx"}


def test_initial_values_key_constant_matches_context_key_pattern():
    """The exported key constant must satisfy ContextKeySpec's regex
    (dotted DNS-label-style identifier). A typo here would break
    user code that imports it for round-tripping."""
    import re

    # noinspection PyProtectedMember
    from tabsdatak._api import CONTEXT_KEY_RE_STR

    assert re.match(CONTEXT_KEY_RE_STR, DATABRICKS_INITIAL_VALUES_KEY)
