from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
from sqlalchemy.exc import OperationalError, ProgrammingError

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    UserPassword,
)
from tabsdatak.conn.starrocks import S3Location, StarRocksDestConn, StarRocksDest
# noinspection PyProtectedMember
from tabsdatak.conn.starrocks import _impl as impl
# noinspection PyProtectedMember
from tabsdatak.conn.starrocks import _plugin as plugin
from tabsdatak.conn.starrocks.error import (
    StarRocksErrorCode,
    StarRocksRuntimeException,
)


pytestmark = pytest.mark.starrocks


# ---------------------------------------------------------------------------
# Test context shims for DestContext
# ---------------------------------------------------------------------------


@dataclass
class _Ctx:
    _work: Path

    @property
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
# Fixture builders -- a StarRocksDestConn / StarRocksDest pointing at a bogus
# host. Engine connect is mocked in every test, so the URI never goes
# through a real driver.
# ---------------------------------------------------------------------------


def _staging() -> S3Location:
    return S3Location(
        bucket="staging-bucket",
        region="us-east-1",
        credentials=AwsAccessSecretKey(
            access_key_id=Secret("AKIA..."),
            secret_access_key=Secret("secret"),
        ),
        base_path="stg/",
    )


def _bare_conn() -> StarRocksDestConn:
    return StarRocksDestConn(
        host="stub.invalid",
        port=9030,
        database="testdb",
        staging=_staging(),
        credentials=UserPassword(
            user=Secret("u"),
            password=Secret("p"),
        ),
    )


# ---------------------------------------------------------------------------
# StarRocksDestPlugin.write_out -- error wrapping (fault injection via mocks)
# ---------------------------------------------------------------------------


def test_dest_plugin_raises_starrocks_3_on_tables_slots_mismatch(work_dir):
    dest = StarRocksDest(
        tables=["analytics.users", "analytics.orders"],
    )
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(StarRocksRuntimeException) as ei:
        plugin.StarRocksDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_conn(), dest, [src]
        )
    assert ei.value.api_error is StarRocksErrorCode.STARROCKS_3


def test_dest_plugin_wraps_operational_error_as_starrocks_4(monkeypatch, work_dir):
    # Skip the actual staging upload step.
    monkeypatch.setattr(impl, "upload_parquet", lambda *a, **kw: "s3://x/y")

    fake = MagicMock()
    fake.begin.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(impl, "build_starrocks_engine", lambda **kw: fake)

    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = StarRocksDest(tables=["users"])
    with pytest.raises(StarRocksRuntimeException) as ei:
        plugin.StarRocksDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_conn(), dest, [src]
        )
    assert ei.value.api_error is StarRocksErrorCode.STARROCKS_4
    fake.dispose.assert_called()


def test_dest_plugin_wraps_other_sqlalchemy_error_as_starrocks_5(
    monkeypatch, work_dir,
):
    monkeypatch.setattr(impl, "upload_parquet", lambda *a, **kw: "s3://x/y")

    fake = MagicMock()
    fake.begin.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(impl, "build_starrocks_engine", lambda **kw: fake)

    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = StarRocksDest(tables=["users"])
    with pytest.raises(StarRocksRuntimeException) as ei:
        plugin.StarRocksDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_conn(), dest, [src]
        )
    assert ei.value.api_error is StarRocksErrorCode.STARROCKS_5


# ---------------------------------------------------------------------------
# Happy path: ensure the plugin orchestrates upload + SQL the right way
# ---------------------------------------------------------------------------


def test_dest_plugin_uploads_then_executes_insert(monkeypatch, work_dir):
    calls: list[tuple] = []

    def fake_upload(staging, work, upload_id, parquet, table_key):
        calls.append(("upload", str(parquet.name), table_key))
        return f"s3://staging-bucket/stg/{upload_id}/{table_key}/{parquet.name}"

    monkeypatch.setattr(impl, "upload_parquet", fake_upload)

    executed: list[str] = []

    class _FakeConn:
        def execute(self, stmt):
            executed.append(str(stmt))

    class _BeginCtx:
        def __enter__(self):
            return _FakeConn()

        def __exit__(self, *a):
            return False

    fake_engine = MagicMock()
    fake_engine.begin.side_effect = lambda: _BeginCtx()
    monkeypatch.setattr(impl, "build_starrocks_engine", lambda **kw: fake_engine)

    parquet = _make_parquet(work_dir / "users.parquet", [{"id": 1, "name": "a"}])
    dest = StarRocksDest(tables=["users"])
    plugin.StarRocksDestPlugin().write_out(
        _dest_ctx(work_dir), _bare_conn(), dest, [parquet]
    )

    assert calls == [("upload", "users.parquet", "users")]
    assert len(executed) == 1
    assert executed[0].startswith("INSERT INTO `testdb`.`users` SELECT * FROM FILES(")
    fake_engine.dispose.assert_called()


def test_dest_plugin_replace_truncates_then_inserts(monkeypatch, work_dir):
    monkeypatch.setattr(
        impl,
        "upload_parquet",
        lambda *a, **kw: "s3://staging-bucket/stg/x/users/u.parquet",
    )

    executed: list[str] = []

    class _FakeConn:
        def execute(self, stmt):
            executed.append(str(stmt))

    class _BeginCtx:
        def __enter__(self):
            return _FakeConn()

        def __exit__(self, *a):
            return False

    fake_engine = MagicMock()
    fake_engine.begin.side_effect = lambda: _BeginCtx()
    monkeypatch.setattr(impl, "build_starrocks_engine", lambda **kw: fake_engine)

    parquet = _make_parquet(work_dir / "users.parquet", [{"id": 1}])
    dest = StarRocksDest(
        tables=["users"],
        if_table_exists="replace",
    )
    plugin.StarRocksDestPlugin().write_out(
        _dest_ctx(work_dir), _bare_conn(), dest, [parquet]
    )

    assert len(executed) == 2
    assert executed[0] == "TRUNCATE TABLE `testdb`.`users`"
    assert executed[1].startswith("INSERT INTO `testdb`.`users` SELECT * FROM FILES(")


# ---------------------------------------------------------------------------
# Explorer stubs
# ---------------------------------------------------------------------------


def test_explorer_methods_are_stubs():
    exp = plugin.StarRocksExplorer()
    assert exp.metadata(_bare_conn()) is None
    assert exp.data(_bare_conn(), "SELECT 1") is None


# ---------------------------------------------------------------------------
# DestDef wiring
# ---------------------------------------------------------------------------


def test_starrocks_dest_def_wires_plugin_and_explorer():
    assert plugin.STARROCKS_DEST.conn is StarRocksDestConn
    assert plugin.STARROCKS_DEST.dest is StarRocksDest
    assert plugin.STARROCKS_DEST.plugin is plugin.StarRocksDestPlugin
    assert plugin.STARROCKS_DEST.explorer is plugin.StarRocksExplorer
    assert plugin.STARROCKS_DEST.type_ == "starrocks-bulk-out"


def test_starrocks_dest_cardinality_uses_tables_length():
    dest = StarRocksDest(
        tables=["users", "orders", "events"],
    )
    assert plugin.STARROCKS_DEST.cardinality(dest) == 3


# ---------------------------------------------------------------------------
# _resolve_dest_cfg
# ---------------------------------------------------------------------------


def test_resolve_dest_cfg_returns_defaults_when_empty():
    import logging as _logging
    cfg = plugin._resolve_dest_cfg(None)
    assert cfg.logging_level == _logging.ERROR


def test_resolve_dest_cfg_applies_overrides():
    cfg = plugin._resolve_dest_cfg({"starrocks.logging_level": 20})
    assert cfg.logging_level == 20


def test_resolve_dest_cfg_warns_on_unknown_key_via_raw_dict(caplog):
    with caplog.at_level("WARNING", logger=plugin.logger.name):
        plugin._resolve_dest_cfg({"bogus.key": "value"})
    assert any("Unknown dest_cfg key" in r.getMessage() for r in caplog.records)
