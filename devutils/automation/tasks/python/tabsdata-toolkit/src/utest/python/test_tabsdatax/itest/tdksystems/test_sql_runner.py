#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for tabsdatax.itest.tdksystems.sql_runner — uses an in-memory SQLite engine so we
exercise the real SQLAlchemy code path without any external DB."""
from __future__ import annotations

import io
import os
from pathlib import Path

import pytest

from tabsdatax.itest.tdksystems.errors import DatagenError
from tabsdatax.itest.tdksystems.sql_runner import (
    _is_only_comments,
    _print_result,
    run_sql_file,
    run_sql_text,
    split_sql_statements,
)


@pytest.fixture
def sqlite_uri(tmp_path) -> str:
    # File-backed sqlite — each run_sql_text call opens a fresh connection,
    # and the file persists state across calls within one test.
    return f"sqlite:///{tmp_path / 'test.db'}"


def _capture_run(uri: str, sql: str) -> str:
    sink = io.StringIO()
    run_sql_text(uri, sql, out=sink)
    return sink.getvalue()


# ---- DDL / DML produce no output ------------------------------------------

def test_run_sql_text_ddl_produces_no_output(sqlite_uri):
    out = _capture_run(sqlite_uri, "CREATE TABLE t (id INT, name TEXT)")
    assert out == ""


def test_run_sql_text_insert_produces_no_output(sqlite_uri):
    _capture_run(sqlite_uri, "CREATE TABLE t (id INT, name TEXT)")
    out = _capture_run(sqlite_uri, "INSERT INTO t VALUES (1, 'a'), (2, 'b')")
    assert out == ""


# ---- SELECT prints header + rows as TSV -----------------------------------

def test_run_sql_text_select_prints_header_and_rows(sqlite_uri):
    _capture_run(sqlite_uri,
                 "CREATE TABLE t (id INT, name TEXT);"
                 "INSERT INTO t VALUES (1, 'alpha'), (2, 'beta');")
    out = _capture_run(sqlite_uri, "SELECT id, name FROM t ORDER BY id")
    lines = out.rstrip("\n").splitlines()
    assert lines[0] == "id\tname"
    assert lines[1] == "1\talpha"
    assert lines[2] == "2\tbeta"
    assert len(lines) == 3


def test_run_sql_text_select_no_rows_prints_header_only(sqlite_uri):
    _capture_run(sqlite_uri, "CREATE TABLE t (id INT)")
    out = _capture_run(sqlite_uri, "SELECT id FROM t")
    assert out.rstrip("\n") == "id"


def test_run_sql_text_renders_null_as_empty(sqlite_uri):
    _capture_run(sqlite_uri,
                 "CREATE TABLE t (id INT, name TEXT);"
                 "INSERT INTO t VALUES (1, NULL), (2, 'x');")
    out = _capture_run(sqlite_uri, "SELECT id, name FROM t ORDER BY id")
    lines = out.rstrip("\n").splitlines()
    # Tab-separated; the NULL value renders as empty string between tabs.
    assert lines[1] == "1\t"
    assert lines[2] == "2\tx"


# ---- Multi-statement batches: each result set rendered ---------------------

def test_run_sql_text_multiple_selects_print_each(sqlite_uri):
    _capture_run(sqlite_uri,
                 "CREATE TABLE a (x INT);"
                 "CREATE TABLE b (y INT);"
                 "INSERT INTO a VALUES (1);"
                 "INSERT INTO b VALUES (10);")
    out = _capture_run(sqlite_uri, "SELECT x FROM a; SELECT y FROM b;")
    lines = out.rstrip("\n").splitlines()
    assert lines == ["x", "1", "y", "10"]


# ---- Errors are surfaced as DatagenError, not SQLAlchemyError --------------

def test_run_sql_text_raises_DatagenError_on_invalid_sql(sqlite_uri):
    with pytest.raises(DatagenError, match="SQL error"):
        _capture_run(sqlite_uri, "SELEC 1")


def test_run_sql_text_raises_when_uri_empty():
    with pytest.raises(DatagenError, match="SQLALCHEMY_URI is empty"):
        run_sql_text("", "SELECT 1")


# ---- run_sql_file thin wrapper ---------------------------------------------

def test_run_sql_file_dispatches_to_text(sqlite_uri, tmp_path):
    f = tmp_path / "q.sql"
    f.write_text("CREATE TABLE t (id INT); INSERT INTO t VALUES (7);")
    run_sql_file(sqlite_uri, f)
    sink = io.StringIO()
    run_sql_text(sqlite_uri, "SELECT id FROM t", out=sink)
    assert sink.getvalue().rstrip("\n") == "id\n7"


def test_run_sql_file_missing_path_errors(tmp_path):
    with pytest.raises(DatagenError, match="SQL file does not exist"):
        run_sql_file("sqlite:///:memory:", tmp_path / "nope.sql")


# ---- engine disposal: no leaked connection to the (per-domain) DB ----------


def _spy_create_engine(monkeypatch):
    """Patch sqlalchemy.create_engine to record every engine made and whether it
    was disposed. A leaked engine keeps a live connection to the per-domain
    database, which blocks the DROP DATABASE that create-cleanup / destroy runs
    (and orphans the DB, masking the real error as a later 'already exists')."""
    import sqlalchemy

    real_create = sqlalchemy.create_engine
    made: list = []
    disposed: list = []

    def spy(*args, **kwargs):
        eng = real_create(*args, **kwargs)
        original = eng.dispose

        def tracked(*a, **k):
            disposed.append(eng)
            return original(*a, **k)

        eng.dispose = tracked
        made.append(eng)
        return eng

    monkeypatch.setattr(sqlalchemy, "create_engine", spy)
    return made, disposed


def test_run_sql_text_disposes_engine_on_success(sqlite_uri, monkeypatch):
    made, disposed = _spy_create_engine(monkeypatch)
    run_sql_text(sqlite_uri, "CREATE TABLE t (id INT)")
    assert len(made) == 1
    assert made[0] in disposed


def test_run_sql_text_disposes_engine_on_error(sqlite_uri, monkeypatch):
    made, disposed = _spy_create_engine(monkeypatch)
    with pytest.raises(DatagenError, match="SQL error"):
        run_sql_text(sqlite_uri, "SELEC bad")
    assert len(made) == 1
    assert made[0] in disposed


# ---- `:word` in SQL is literal, not a bind parameter -----------------------


def test_bind_token_in_comment_is_not_a_bind_parameter(sqlite_uri):
    # A model DDL comment such as `WHERE sim_counter > :last LIMIT :size` must run
    # as-is — regression for "A value is required for bind parameter 'last'" when
    # SQLAlchemy's text() treated `:word` in a comment as a bind. A block comment
    # survives both splitter paths (sqlparse and the comment-stripping fallback).
    run_sql_text(
        sqlite_uri,
        "CREATE TABLE t (id INT) /* read: WHERE id > :last LIMIT :size */",
    )
    sink = io.StringIO()
    run_sql_text(sqlite_uri, "SELECT count(*) AS n FROM t", out=sink)
    assert sink.getvalue().rstrip("\n") == "n\n0"


# ---- comment-only fragments are dropped (no "empty query") -----------------


def test_is_only_comments():
    assert _is_only_comments("-- just a line comment")
    assert _is_only_comments("/* just a block comment */")
    assert _is_only_comments("   \n  ")
    assert not _is_only_comments("SELECT 1")
    assert not _is_only_comments("-- lead\nSELECT 1")


def test_split_drops_comment_only_fragments():
    frags = split_sql_statements(
        "CREATE TABLE t (id INT); /* standalone comment */; SELECT 1;"
    )
    assert len(frags) == 2
    assert frags[0].startswith("CREATE TABLE")
    assert frags[1].startswith("SELECT")


def test_run_sql_text_tolerates_comment_only_fragment(sqlite_uri):
    # A comment-only fragment between statements must not raise "can't execute an
    # empty query" — it is skipped.
    run_sql_text(
        sqlite_uri,
        "CREATE TABLE t (id INT); /* a note */; INSERT INTO t VALUES (1);",
    )
    sink = io.StringIO()
    run_sql_text(sqlite_uri, "SELECT id FROM t", out=sink)
    assert sink.getvalue().rstrip("\n") == "id\n1"


# ---- _print_result low-level test (skipped: covered indirectly above) ------
# We cover _print_result through run_sql_text; nothing additional here.