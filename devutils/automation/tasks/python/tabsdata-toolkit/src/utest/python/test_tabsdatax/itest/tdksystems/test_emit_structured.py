"""Tests for the emit additions backing the kafka + saas-sql loaders:
the schema sidecar (gated on TD_GEN_SCHEMA_SIDECAR), read_structured, and
batched iter_insert_statements."""
from __future__ import annotations

from tabsdatax.itest.tdksystems import emit
from tabsdatax.itest.tdksystems.emit import Column, Table


def _tables():
    return [Table(
        "t1",
        [Column("id", "int", nullable=False, auto=True),
         Column("name", "varchar(5)")],
        [{"id": 1, "name": "a"}, {"id": 2, "name": None}],
    )]


def test_iter_insert_statements_batches_and_skips_auto():
    cols = [Column("id", "int", auto=True),
            Column("name", "varchar(5)", nullable=False),
            Column("n", "int")]
    rows = [{"id": i, "name": f"x{i}", "n": i} for i in range(5)]
    stmts = list(emit.iter_insert_statements("t", cols, rows, batch=2, quote="`"))
    assert len(stmts) == 3                      # 2 + 2 + 1
    # auto 'id' is skipped; only (name,n) inserted.
    assert stmts[0].startswith("INSERT INTO t (name,n) VALUES ")
    assert "id" not in stmts[0].split("VALUES")[0]
    # multi-row: two value tuples in the first (full) batch.
    assert stmts[0].count("),(") == 1


def test_iter_insert_statements_empty_rows():
    cols = [Column("a", "int")]
    assert list(emit.iter_insert_statements("t", cols, [], batch=10)) == []


def test_schema_sidecar_written_only_when_requested(tmp_path):
    out = tmp_path / "no"
    emit.write_tables(_tables(), out, "json")
    assert not (out / "_schema.json").exists()


def test_schema_sidecar_and_read_structured(tmp_path, monkeypatch):
    monkeypatch.setenv("TD_GEN_SCHEMA_SIDECAR", "1")
    out = tmp_path / "yes"
    emit.write_tables(_tables(), out, "json")
    assert (out / "_schema.json").is_file()

    structured = emit.read_structured(out)
    assert len(structured) == 1
    name, cols, rows = structured[0]
    assert name == "t1"
    assert [c.name for c in cols] == ["id", "name"]
    assert cols[0].auto is True and cols[0].nullable is False
    assert len(rows) == 2 and rows[1]["name"] is None
