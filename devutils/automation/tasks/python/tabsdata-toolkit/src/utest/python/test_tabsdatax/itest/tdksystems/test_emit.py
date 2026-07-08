"""Tests for tabsdatax.itest.tdksystems.emit — table chunking + SQL/CSV/JSON writers."""
from __future__ import annotations

from pathlib import Path

import pytest

from tabsdatax.itest.tdksystems.emit import (
    Column, Table, _chunk_tables, write_tables, VALID_FORMATS,
)


# ---- helpers --------------------------------------------------------------

def _table(name: str, n: int) -> Table:
    cols = [Column("id", "int"), Column("name", "varchar(20)")]
    rows = [{"id": i, "name": f"r{i}"} for i in range(n)]
    return Table(name=name, columns=cols, rows=rows)


# ---- _chunk_tables --------------------------------------------------------

def test_chunk_none_yields_one_pair_per_table():
    t = _table("t", 7)
    pairs = _chunk_tables([t], chunks=None)
    assert len(pairs) == 1
    name, view = pairs[0]
    assert name == "t"
    assert view is t


def test_chunks_split_evenly():
    pairs = _chunk_tables([_table("t", 100)], chunks=30)
    # 30+30+30+10 = 4 chunks
    assert len(pairs) == 4
    names = [n for n, _ in pairs]
    assert names == ["t-00001", "t-00002", "t-00003", "t-00004"]
    sizes = [len(v.rows) for _, v in pairs]
    assert sizes == [30, 30, 30, 10]


def test_chunks_exact_multiple():
    pairs = _chunk_tables([_table("t", 60)], chunks=20)
    assert [n for n, _ in pairs] == ["t-00001", "t-00002", "t-00003"]
    assert [len(v.rows) for _, v in pairs] == [20, 20, 20]


def test_chunks_smaller_than_chunk_size():
    pairs = _chunk_tables([_table("t", 5)], chunks=20)
    assert len(pairs) == 1
    assert pairs[0][0] == "t-00001"
    assert len(pairs[0][1].rows) == 5


def test_empty_table_still_emits_one_chunk_when_chunked():
    pairs = _chunk_tables([_table("t", 0)], chunks=20)
    assert len(pairs) == 1
    assert pairs[0][0] == "t-00001"
    assert pairs[0][1].rows == []


def test_sql_post_only_on_last_chunk():
    t = _table("t", 60)
    t.sql_post = ["UPDATE t SET name = 'fixed' WHERE id = 0"]
    pairs = _chunk_tables([t], chunks=20)
    last_view = pairs[-1][1]
    assert last_view.sql_post == t.sql_post
    for _, v in pairs[:-1]:
        assert v.sql_post == []


def test_chunk_view_keeps_original_table_name_for_sql():
    """The basename has -<NNNNN> but the Table.name is the original (so SQL
    INSERT statements reference the right table)."""
    pairs = _chunk_tables([_table("t", 50)], chunks=10)
    for basename, view in pairs:
        assert view.name == "t"
        assert basename.startswith("t-")


# ---- write_tables ---------------------------------------------------------

def test_write_tables_sql_writes_one_file_per_table_with_run_order(tmp_path):
    out = tmp_path / "out"
    write_tables([_table("foo", 3), _table("bar", 2)], out, "sql")
    assert (out / "foo.sql").is_file()
    assert (out / "bar.sql").is_file()
    order = (out / "run-order.txt").read_text().splitlines()
    assert order == ["foo.sql", "bar.sql"]


def test_write_tables_chunked_sql_lists_every_chunk_in_run_order(tmp_path):
    out = tmp_path / "out"
    write_tables([_table("t", 25)], out, "sql", chunks=10)
    files = sorted(p.name for p in out.glob("t-*.sql"))
    assert files == ["t-00001.sql", "t-00002.sql", "t-00003.sql"]
    order = (out / "run-order.txt").read_text().splitlines()
    assert order == files


def test_write_tables_sql_inserts_use_logical_name(tmp_path):
    out = tmp_path / "out"
    write_tables([_table("ents", 5)], out, "sql", chunks=2)
    sql = (out / "ents-00001.sql").read_text()
    assert "INSERT INTO ents " in sql
    # No reference to the chunked filename in SQL.
    assert "ents-00001" not in sql


def test_write_tables_csv_writes_one_file(tmp_path):
    out = tmp_path / "out"
    write_tables([_table("t", 3)], out, "csv")
    csv = (out / "t.csv").read_text()
    assert csv.splitlines()[0] == "id,name"
    assert "r0" in csv and "r2" in csv


def test_write_tables_json_writes_one_file(tmp_path):
    import json
    out = tmp_path / "out"
    write_tables([_table("t", 3)], out, "json")
    rows = json.loads((out / "t.json").read_text())
    assert rows == [{"id": 0, "name": "r0"},
                    {"id": 1, "name": "r1"},
                    {"id": 2, "name": "r2"}]


def test_write_tables_writes_entities_manifest(tmp_path):
    import json
    out = tmp_path / "out"
    write_tables([_table("foo", 3), _table("bar", 2)], out, "csv")
    counts = json.loads((out / "_entities.json").read_text())
    assert counts == {"foo": 3, "bar": 2}


def test_write_tables_rejects_existing_out(tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    with pytest.raises(FileExistsError):
        write_tables([_table("t", 1)], out, "sql")


def test_write_tables_rejects_unknown_format(tmp_path):
    with pytest.raises(ValueError):
        write_tables([_table("t", 1)], tmp_path / "out", "yaml")


def test_write_tables_rejects_bad_chunks(tmp_path):
    with pytest.raises(ValueError):
        write_tables([_table("t", 1)], tmp_path / "out", "sql", chunks=0)
