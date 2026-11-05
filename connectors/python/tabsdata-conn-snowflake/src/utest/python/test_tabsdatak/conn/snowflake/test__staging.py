#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io

import polars as pl
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake import _impl as impl, _staging as staging

from test_tabsdatak.conn.snowflake.support import _FakeCursor

pytestmark = pytest.mark.snowflake

ROWS = pl.LazyFrame({"id": range(200_000), "name": ["x" * 24] * 200_000})


def _read(parts) -> pl.DataFrame:
    return pl.concat(pl.read_parquet(part) for part in parts)


def test_a_small_table_is_one_part():
    parts = list(staging.parquet_parts(ROWS))
    assert len(parts) == 1
    assert _read(parts).equals(ROWS.collect())


def test_a_table_over_the_part_size_is_cut_into_parts_in_order():
    parts = list(staging.parquet_parts(ROWS, part_bytes=64 * 1024))
    assert len(parts) > 1
    assert _read(parts).equals(ROWS.collect())


def test_every_part_carries_the_schema():
    parts = list(staging.parquet_parts(ROWS, part_bytes=64 * 1024))
    assert all(
        pl.read_parquet_schema(part) == ROWS.collect_schema() for part in parts
    )


def test_an_empty_table_still_stages_its_schema():
    empty = pl.LazyFrame(schema={"id": pl.Int64, "name": pl.String})
    [part] = staging.parquet_parts(empty)
    assert pl.read_parquet(part).schema == empty.collect_schema()


def test_the_part_size_is_read_when_the_parts_are_cut(monkeypatch):
    monkeypatch.setattr(staging, "PART_BYTES", 64 * 1024)
    assert len(list(staging.parquet_parts(ROWS))) > 1


def test_every_part_is_put_into_one_folder_from_memory(monkeypatch):
    monkeypatch.setattr(staging, "PART_BYTES", 64 * 1024)
    log: list[str] = []
    cursor = _FakeCursor(log)
    folder = staging.put_frame_to_stage(cursor, ROWS, "stg")
    assert len(log) == len(cursor.streamed) > 1
    assert log[0] == (
        f"PUT 'file:///part-00000.parquet' @stg/{folder} AUTO_COMPRESS = FALSE"
    )
    assert _read(io.BytesIO(data) for data in cursor.streamed).equals(ROWS.collect())
    assert staging.first_part(folder) == f"{folder}/part-00000.parquet"


def test_a_failed_put_removes_what_was_staged(monkeypatch):
    monkeypatch.setattr(staging, "PART_BYTES", 64 * 1024)
    log: list[str] = []

    class _Failing(_FakeCursor):
        def execute(self, sql, params=None, file_stream=None):
            if sql.startswith("PUT 'file:///part-00002"):
                raise RuntimeError("put failed")
            super().execute(sql, params, file_stream)

    with pytest.raises(RuntimeError, match="put failed"):
        staging.put_frame_to_stage(_Failing(log), ROWS, "stg")
    assert log[-1].startswith("REMOVE @stg/")


def test_a_failed_removal_is_only_logged(caplog):
    class _Refusing(_FakeCursor):
        def execute(self, sql, params=None, file_stream=None):
            raise RuntimeError("denied")

    with caplog.at_level("WARNING", logger=staging.logger.name):
        staging.remove_from_stage(_Refusing([]), "stg", "f")
    assert any("@stg/f" in r.getMessage() for r in caplog.records)


def test_the_copy_loads_the_whole_folder():
    log: list[str] = []
    staging.copy_into(
        _FakeCursor(log), "stg", "f", "T", match_by_column_name="X", purge="TRUE"
    )
    assert log[0].startswith("COPY INTO T FROM @stg/f/ ")


def test_the_temporary_stage_skips_client_side_encryption():
    log: list[str] = []
    impl._create_stage_if_not_exists(_FakeCursor(log), "tmp", is_temp=True)
    assert log == [
        "CREATE TEMPORARY STAGE IF NOT EXISTS tmp"
        " ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')"
    ]


def test_a_named_stage_is_left_as_its_owner_made_it():
    log: list[str] = []
    impl._create_stage_if_not_exists(_FakeCursor(log), "mine", is_temp=False)
    assert log == ["CREATE STAGE IF NOT EXISTS mine"]


def test_a_failed_load_removes_the_staged_parts():
    log: list[str] = []
    with pytest.raises(RuntimeError, match="copy failed"):
        with staging.staged_frame(_FakeCursor(log), ROWS, "stg") as folder:
            raise RuntimeError("copy failed")
    assert log[-1] == f"REMOVE @stg/{folder}/"


def test_a_load_that_ends_leaves_the_parts_to_the_copy_purge():
    log: list[str] = []
    with staging.staged_frame(_FakeCursor(log), ROWS, "stg"):
        pass
    assert not any(statement.startswith("REMOVE") for statement in log)
