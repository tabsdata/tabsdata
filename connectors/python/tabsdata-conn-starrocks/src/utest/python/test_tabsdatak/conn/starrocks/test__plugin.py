#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
from sqlalchemy.exc import OperationalError, ProgrammingError

from tabsdatak.api import Secret
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    UserPassword,
)

# noinspection PyProtectedMember
# noinspection PyProtectedMember
from tabsdatak.conn.starrocks import (
    S3Location,
    StarRocksDest,
    StarRocksDestConn,
    _impl as impl,
    _plugin as plugin,
)
from tabsdatak.conn.starrocks._explorer import StarRocksConnectionExplorer
from tabsdatak.conn.starrocks.error import (
    StarRocksErrorCode,
    StarRocksRuntimeException,
)

pytestmark = pytest.mark.starrocks


@pytest.fixture(autouse=True)
def _evolutions(monkeypatch):
    seen: list[tuple[str, str]] = []

    class _Evolver:
        def __init__(self, conn, database, table):
            pass

        def evolve(self, target, files_fn, evolution, footer=None):
            seen.append((target, evolution))
            return []

    monkeypatch.setattr(impl, "StarRocksSchemaEvolver", _Evolver)
    return seen


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


def _staged(monkeypatch, calls: list | None = None) -> None:
    def stage(staging, store, frame, rel):
        if calls is not None:
            calls.append(rel)
        return f"stg/{rel}", f"s3://staging-bucket/stg/{rel}"

    monkeypatch.setattr(impl, "stage", stage)
    monkeypatch.setattr(impl, "discard", lambda store, keys: None)
    # Nothing is staged, so there is no footer to read.
    monkeypatch.setattr(impl, "_footer", lambda store, key: None)


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
        base_path="/stg",
    )


def _bare_conn() -> StarRocksDestConn:
    return StarRocksDestConn(
        host="stub.invalid",
        port="9030",
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
    src = pl.LazyFrame([{"id": 1}])
    with pytest.raises(StarRocksRuntimeException) as ei:
        plugin.StarRocksDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_conn(), dest, [src]
        )
    assert ei.value.api_error is StarRocksErrorCode.STARROCKS_3


def test_dest_plugin_wraps_operational_error_as_starrocks_4(monkeypatch, work_dir):
    _staged(monkeypatch)

    fake = MagicMock()
    fake.begin.side_effect = OperationalError("conn fail", {}, BaseException("c"))
    monkeypatch.setattr(impl, "build_starrocks_engine", lambda **kw: fake)

    src = pl.LazyFrame([{"id": 1}])
    dest = StarRocksDest(tables=["users"])
    with pytest.raises(StarRocksRuntimeException) as ei:
        plugin.StarRocksDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_conn(), dest, [src]
        )
    assert ei.value.api_error is StarRocksErrorCode.STARROCKS_4
    fake.dispose.assert_called()


def test_dest_plugin_wraps_other_sqlalchemy_error_as_starrocks_5(
    monkeypatch,
    work_dir,
):
    _staged(monkeypatch)

    fake = MagicMock()
    fake.begin.side_effect = ProgrammingError("bad sql", {}, BaseException("p"))
    monkeypatch.setattr(impl, "build_starrocks_engine", lambda **kw: fake)

    src = pl.LazyFrame([{"id": 1}])
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

    _staged(monkeypatch, calls)

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

    table = pl.LazyFrame([{"id": 1, "name": "a"}])
    dest = StarRocksDest(tables=["users"])
    plugin.StarRocksDestPlugin().write_out(
        _dest_ctx(work_dir), _bare_conn(), dest, [table]
    )

    [rel] = calls
    assert rel.endswith("/users/0.parquet")
    assert len(executed) == 1
    # The INSERT names the table's columns explicitly.
    assert executed[0].startswith(
        "INSERT INTO `testdb`.`users` (`id`, `name`) SELECT * FROM FILES("
    )
    fake_engine.dispose.assert_called()


def test_dest_plugin_replace_truncates_then_inserts(monkeypatch, work_dir):
    _staged(monkeypatch)

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

    table = pl.LazyFrame([{"id": 1}])
    dest = StarRocksDest(
        tables=["users"],
        if_table_exists="replace",
    )
    plugin.StarRocksDestPlugin().write_out(
        _dest_ctx(work_dir), _bare_conn(), dest, [table]
    )

    assert len(executed) == 2
    assert executed[0] == "TRUNCATE TABLE `testdb`.`users`"
    assert executed[1].startswith(
        "INSERT INTO `testdb`.`users` (`id`) SELECT * FROM FILES("
    )


def test_dest_plugin_insert_names_only_the_columns_of_the_table(monkeypatch, work_dir):
    # Regression: the INSERT must name only the columns the staged table
    # actually carries, so a target column the file omits (e.g. an
    # AUTO_INCREMENT `id`) is left for StarRocks to populate. A bare
    # ``INSERT ... SELECT *`` used to fail with "Inserted target column count: 5
    # doesn't match select/value column count: 4" against such a table.
    _staged(monkeypatch)

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

    # No `id` column -- mirrors the file-seeded `city` table whose target has an
    # AUTO_INCREMENT `id`.
    table = pl.LazyFrame([
        {"name": "Kabul", "country_code": "AFG", "district": "Kabol", "population": 1}
    ])
    dest = StarRocksDest(tables=["city"])
    plugin.StarRocksDestPlugin().write_out(
        _dest_ctx(work_dir), _bare_conn(), dest, [table]
    )

    assert len(executed) == 1
    assert executed[0].startswith(
        "INSERT INTO `testdb`.`city` "
        "(`name`, `country_code`, `district`, `population`) SELECT * FROM FILES("
    )
    assert "`id`" not in executed[0]


def test_dest_plugin_leaves_the_table_of_a_none_slot_untouched(monkeypatch, work_dir):
    calls: list[str] = []
    _staged(monkeypatch, calls)

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

    dest = StarRocksDest(
        tables=["users", "orders", "events"],
        if_table_exists="replace",
    )
    plugin.StarRocksDestPlugin().write_out(
        _dest_ctx(work_dir),
        _bare_conn(),
        dest,
        [pl.LazyFrame([{"id": 1}]), None, pl.LazyFrame([{"id": 3}])],
    )

    assert [rel.split("/")[1] for rel in calls] == ["users", "events"]
    assert not any("orders" in statement for statement in executed)
    assert [s for s in executed if s.startswith("TRUNCATE")] == [
        "TRUNCATE TABLE `testdb`.`users`",
        "TRUNCATE TABLE `testdb`.`events`",
    ]


def test_dest_plugin_writes_nothing_when_every_slot_is_none(monkeypatch, work_dir):
    calls: list[str] = []
    _staged(monkeypatch, calls)
    build = MagicMock()
    monkeypatch.setattr(impl, "build_starrocks_engine", build)
    monkeypatch.setattr(impl, "staging_store", MagicMock(side_effect=AssertionError))

    plugin.StarRocksDestPlugin().write_out(
        _dest_ctx(work_dir),
        _bare_conn(),
        StarRocksDest(tables=["users", "orders"], if_table_exists="replace"),
        [None, None],
    )

    build.assert_not_called()
    assert calls == []


# ---------------------------------------------------------------------------
# DestDef wiring
# ---------------------------------------------------------------------------


def test_starrocks_dest_def_wires_plugin_and_explorer():
    assert plugin.STARROCKS_DEST.conn is StarRocksDestConn
    assert plugin.STARROCKS_DEST.dest is StarRocksDest
    assert plugin.STARROCKS_DEST.plugin is plugin.StarRocksDestPlugin
    assert plugin.STARROCKS_DEST.explorer is StarRocksConnectionExplorer
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
    plugin._resolve_dest_cfg({"bogus.key": "value"})
    assert any("Unknown dest_cfg key" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("engine", ["iceberg", "native"])
def test_dest_takes_no_engine(engine):
    """StarRocks evolves with its one engine; writing any is refused."""
    with pytest.raises(ConnInitException, match="schema_evolution_engine"):
        StarRocksDest(
            tables=["t"], dest_cfg={"starrocks.schema_evolution_engine": engine}
        )


def test_execute_evolves_each_table_before_loading_it(monkeypatch, _evolutions):
    executed: list[str] = []

    class _Conn:
        def execute(self, stmt):
            executed.append(str(stmt))

    class _BeginCtx:
        def __enter__(self):
            return _Conn()

        def __exit__(self, *a):
            return False

    engine = MagicMock()
    engine.begin.side_effect = lambda: _BeginCtx()
    monkeypatch.setattr(impl, "build_starrocks_engine", lambda **kw: engine)
    monkeypatch.setattr(impl, "stage", lambda *a: ("k", "s3://b/k"))
    monkeypatch.setattr(impl, "discard", lambda *a: None)
    monkeypatch.setattr(impl, "_footer", lambda *a: None)
    cfg = impl.StarRocksWriteCfg(
        host="h",
        port=9030,
        database="db",
        credentials=UserPassword(user=Secret("u"), password=Secret("p")),
        staging=_staging(),
        target_tables=["users"],
        schema_evolution="update",
    )
    impl.execute(cfg, [pl.LazyFrame({"id": [1]})], upload_id="up1")
    assert _evolutions == [("`db`.`users`", "update")]
