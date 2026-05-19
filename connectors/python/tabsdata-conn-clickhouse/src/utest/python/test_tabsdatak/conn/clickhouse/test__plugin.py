from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AzureAccountKey,
    GCPHmac,
    GCPJson,
    UserPassword,
)
from tabsdatak.conn.clickhouse import (
    AzBlobLocation,
    ClickHouseConn,
    ClickHouseDest,
    GCSLocation,
    S3Location,
)
# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse import _impl as impl
# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse import _plugin as plugin
from tabsdatak.conn.clickhouse.error import (
    ClickHouseErrorCode,
    ClickHouseRuntimeException,
    ClickHouseValidateException,
)


pytestmark = pytest.mark.clickhouse


# ---------------------------------------------------------------------------
# Test context shims for DestContext (duck-typed; the unit-test surface
# only touches ``work_dir`` and the SPI typing in the plugin call-site)
# ---------------------------------------------------------------------------


@dataclass
class _Ctx:
    _work: Path

    def work_dir(self) -> Path:
        return self._work


def _dest_ctx(work: Path) -> _Ctx:
    return _Ctx(work)


def _make_parquet(path: Path, rows: list[dict]) -> Path:
    pl.DataFrame(rows).write_parquet(path, use_pyarrow=True)
    return path


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    out = tmp_path / "work"
    out.mkdir()
    return out


# ---------------------------------------------------------------------------
# Bare conn / dest helpers -- the staging / credentials never see a real
# clickhouse-connect or boto3 (the bulk-load path is mocked).
# ---------------------------------------------------------------------------


def _s3_staging() -> S3Location:
    return S3Location(
        bucket="staging-bucket",
        region="us-east-1",
        credentials=AwsAccessSecretKey(
            access_key_id=Secret.lit("AKIA..."),
            secret_access_key=Secret.lit("secret"),
        ),
        base_path="staging/",
    )


def _azure_staging() -> AzBlobLocation:
    return AzBlobLocation(
        container="staging-container",
        credentials=AzureAccountKey(
            account_name=Secret.lit("acct"),
            account_key=Secret.lit("key=="),
        ),
        base_path="staging/",
    )


def _gcs_staging() -> GCSLocation:
    return GCSLocation(
        bucket="staging-bucket",
        credentials=GCPJson(json=Secret.lit("{}")),
        hmac_credentials=GCPHmac(
            access_key=Secret.lit("GOOG..."),
            secret_key=Secret.lit("secret"),
        ),
        base_path="staging/",
    )


def _bare_conn(staging=None) -> ClickHouseConn:
    return ClickHouseConn(
        host="ch.example.invalid",
        staging=staging or _s3_staging(),
        port=8123,
        database="analytics",
        credentials=UserPassword(
            user=Secret.lit("u"),
            password=Secret.lit("p"),
        ),
    )


# ---------------------------------------------------------------------------
# ClickHouseDest.validate -- cross-field checks (CLICKHOUSE_3)
# ---------------------------------------------------------------------------


def test_dest_validate_rejects_empty_tables():
    with pytest.raises(ClickHouseValidateException) as ei:
        ClickHouseDest(conn=_bare_conn(), tables=[])
    assert ei.value.api_error is ClickHouseErrorCode.CLICKHOUSE_3


# ---------------------------------------------------------------------------
# _ClickHouseDestPlugin.write_out -- defensive recheck (CLICKHOUSE_4)
# ---------------------------------------------------------------------------


def test_dest_plugin_raises_clickhouse_4_on_tables_slots_mismatch(work_dir):
    dest = ClickHouseDest(
        conn=_bare_conn(),
        tables=["users", "orders"],
    )
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(ClickHouseRuntimeException) as ei:
        plugin._ClickHouseDestPlugin().write_out(
            _dest_ctx(work_dir), dest, [src],
        )
    assert ei.value.api_error is ClickHouseErrorCode.CLICKHOUSE_4


# ---------------------------------------------------------------------------
# _ClickHouseDestPlugin.write_out -- error wrapping (fault injection via
# impl.write_via_bulk_loader monkey-patch).
# ---------------------------------------------------------------------------


class _StubDatabaseError(Exception):
    """Class-named ``DatabaseError`` so ``_is_connection_error``
    classifies it as a connect failure without importing the real
    ``clickhouse_connect`` package."""


_StubDatabaseError.__name__ = "DatabaseError"


def test_dest_plugin_wraps_connection_failure_as_clickhouse_5(
    monkeypatch, work_dir,
):
    def boom(*_a, **_kw):
        raise _StubDatabaseError("connect fail")
    monkeypatch.setattr(impl, "write_via_bulk_loader", boom)

    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = ClickHouseDest(conn=_bare_conn(), tables=["users"])
    with pytest.raises(ClickHouseRuntimeException) as ei:
        plugin._ClickHouseDestPlugin().write_out(
            _dest_ctx(work_dir), dest, [src],
        )
    assert ei.value.api_error is ClickHouseErrorCode.CLICKHOUSE_5


def test_dest_plugin_wraps_other_failure_as_clickhouse_6(
    monkeypatch, work_dir,
):
    monkeypatch.setattr(
        impl,
        "write_via_bulk_loader",
        MagicMock(side_effect=RuntimeError("load fail")),
    )
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = ClickHouseDest(conn=_bare_conn(), tables=["users"])
    with pytest.raises(ClickHouseRuntimeException) as ei:
        plugin._ClickHouseDestPlugin().write_out(
            _dest_ctx(work_dir), dest, [src],
        )
    assert ei.value.api_error is ClickHouseErrorCode.CLICKHOUSE_6


def test_dest_plugin_wraps_unsupported_staging_as_clickhouse_7(
    monkeypatch, work_dir,
):
    monkeypatch.setattr(
        impl,
        "write_via_bulk_loader",
        MagicMock(
            side_effect=TypeError("Unsupported ClickHouse staging location"),
        ),
    )
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = ClickHouseDest(conn=_bare_conn(), tables=["users"])
    with pytest.raises(ClickHouseRuntimeException) as ei:
        plugin._ClickHouseDestPlugin().write_out(
            _dest_ctx(work_dir), dest, [src],
        )
    assert ei.value.api_error is ClickHouseErrorCode.CLICKHOUSE_7


# ---------------------------------------------------------------------------
# _ClickHouseDestPlugin.write_out -- happy path dispatch (the impl is
# mocked; we just verify the plugin forwards the right arguments).
# ---------------------------------------------------------------------------


def test_dest_plugin_dispatches_to_bulk_loader(monkeypatch, work_dir):
    captured: dict = {}

    def stub(cfg, tables):
        captured["cfg"] = cfg
        captured["tables"] = tables
    monkeypatch.setattr(impl, "write_via_bulk_loader", stub)

    src1 = _make_parquet(work_dir / "u.parquet", [{"id": 1}])
    src2 = _make_parquet(work_dir / "o.parquet", [{"id": 2}])
    dest = ClickHouseDest(
        conn=_bare_conn(),
        tables=["users", "orders"],
        if_table_exists="replace",
        schema_evolution="iceberg",
    )
    plugin._ClickHouseDestPlugin().write_out(
        _dest_ctx(work_dir), dest, [src1, src2],
    )
    assert captured["tables"] == [src1, src2]
    assert captured["cfg"].target_tables == ["users", "orders"]
    assert captured["cfg"].if_table_exists == "replace"
    assert captured["cfg"].schema_evolution == "iceberg"


# ---------------------------------------------------------------------------
# Explorer stubs
# ---------------------------------------------------------------------------


def test_explorer_methods_are_stubs():
    exp = plugin._ClickHouseExplorer()
    assert exp.metadata(_bare_conn()) is None
    assert exp.data(_bare_conn(), "SELECT 1") is None


# ---------------------------------------------------------------------------
# DestDef wiring
# ---------------------------------------------------------------------------


def test_clickhouse_dest_def_wires_plugin_and_explorer():
    assert plugin.CLICKHOUSE_DEST.conn is ClickHouseConn
    assert plugin.CLICKHOUSE_DEST.dest is ClickHouseDest
    assert plugin.CLICKHOUSE_DEST.plugin is plugin._ClickHouseDestPlugin
    assert plugin.CLICKHOUSE_DEST.explorer is plugin._ClickHouseExplorer
    assert plugin.CLICKHOUSE_DEST.dest_type == "clickhouse"


def test_clickhouse_dest_cardinality_uses_tables_length():
    dest = ClickHouseDest(
        conn=_bare_conn(),
        tables=["users", "orders", "events"],
    )
    assert plugin.CLICKHOUSE_DEST.cardinality(dest) == 3


# ---------------------------------------------------------------------------
# _is_connection_error classifier
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    [
        "DatabaseError",
        "OperationalError",
        "InterfaceError",
        "ConnectionError",
    ],
)
def test_is_connection_error_recognises_known_names(name):
    cls = type(name, (Exception,), {})
    assert plugin._is_connection_error(cls()) is True


def test_is_connection_error_rejects_other_names():
    assert plugin._is_connection_error(RuntimeError()) is False
    assert plugin._is_connection_error(ValueError()) is False
