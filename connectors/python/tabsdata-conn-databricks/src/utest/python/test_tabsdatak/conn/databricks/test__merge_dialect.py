#
# Copyright 2026 Tabsdata Inc.
#

"""Databricks' merge dialect, and the shared driver run against it.

What Databricks contributes to the merge: the statement's shape, how it stages a
batch through a volume, and the fact that it evolves the target from inside the
``MERGE`` rather than ALTERing it beforehand. The shared pieces are exercised
through this dialect rather than a stub, so the wiring between the two is tested
and not assumed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge._batch import coalesced_batch, incoming_column

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge._driver import write_columns

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge._statement import render_merge

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge.dialect import Staged
from tabsdatak.conn.common.sql.cdc.constants import CdcOperation
from tabsdatak.conn.databricks import MergeCfg

# noinspection PyProtectedMember
from tabsdatak.conn.databricks._merge_dialect import DatabricksMergeDialect
from tabsdatak.conn.databricks.error import (
    DatabricksErrorCode,
    DatabricksRuntimeException,
)

_TARGET = "`main`.`analytics`.`orders`"
_SCRATCH = "`main`.`analytics`.`orders_ab12cd34`"

# ``DESCRIBE TABLE`` reports more than the two columns the connector reads, and
# not in that order, so the fake answers in that shape: a reader going by
# position instead of by name in ``description`` fails here.
_DESCRIBE_FIELDS = ("comment", "col_name", "data_type")


class _RecordingCursor:
    """Captures every statement, and answers a ``DESCRIBE TABLE`` from
    ``columns``."""

    def __init__(self, columns: list[tuple[str, str]] | None = None) -> None:
        self.log: list[tuple[str, dict]] = []
        self._columns = columns or []
        self.description: list[tuple] = []
        self._rows: list[tuple] = []

    def execute(self, statement: str, **parameters) -> None:
        self.log.append((statement, parameters))
        if statement.startswith("DESCRIBE TABLE"):
            self.description = [(field,) for field in _DESCRIBE_FIELDS]
            self._rows = [("", name, type_) for name, type_ in self._columns]
            # The partition / metadata block DESCRIBE appends after the columns.
            self._rows.append(("", "", ""))
            return
        self.description, self._rows = [], []

    def fetchall(self) -> list[tuple]:
        return list(self._rows)

    @property
    def statements(self) -> list[str]:
        return [statement for statement, _ in self.log]


class _FakeWorkspaceClient:
    def __init__(self) -> None:
        self.uploaded: list[str] = []
        self.deleted: list[str] = []
        self.files = self

    def upload(self, target: str, _stream) -> None:
        self.uploaded.append(target)

    def delete(self, target: str) -> None:
        self.deleted.append(target)


def _conn():
    from tabsdatak.api import Secret
    from tabsdatak.conn.databricks import DatabricksDestConn

    return DatabricksDestConn(
        host_url=Secret("https://example.cloud.databricks.com"),
        token=Secret("tok"),
        volume=Secret("main.analytics.staging"),
        warehouse_id=Secret("0123456789abcdef"),
    )


def _dialect(
    cursor: _RecordingCursor,
    *,
    schema_evolution: str = "update",
    ws_client=None,
) -> DatabricksMergeDialect:
    return DatabricksMergeDialect(
        cursor,
        ws_client or _FakeWorkspaceClient(),
        _conn(),
        "main.analytics.orders",
        schema_evolution=schema_evolution,
    )


def _staged(columns: list[tuple[str, str]], relation: str = _SCRATCH) -> Staged:
    return Staged(relation=relation, columns=list(columns))


def _cfg(on=("order_id",), op_column="op") -> MergeCfg:
    return MergeCfg(on=list(on), op_column=op_column)


def _index_starting_with(statements: list[str], prefix: str) -> int:
    """The position of the one statement starting with ``prefix``.

    Selected by prefix rather than by index so a statement added to the merge
    sequence shifts nothing here.
    """
    matches = [i for i, s in enumerate(statements) if s.startswith(prefix)]
    assert len(matches) == 1, f"{prefix!r} matched {len(matches)}: {statements}"
    return matches[0]


def _only_starting_with(statements: list[str], prefix: str) -> str:
    return statements[_index_starting_with(statements, prefix)]


# ---------------------------------------------------------------------------
# The MERGE statement
#
# The marker gates all three clauses. DELETE is emitted first because Databricks
# takes the first matching WHEN MATCHED, so a row marked 'd' is deleted rather
# than falling through to the update.
# ---------------------------------------------------------------------------


def _merge_sql(
    on: list[str] | None = None,
    op_column: str = "op",
    schema_evolution: str = "strict",
) -> str:
    dialect = _dialect(_RecordingCursor(), schema_evolution=schema_evolution)
    return render_merge(
        dialect,
        _TARGET,
        _staged([], relation=_SCRATCH),
        _cfg(on=on or ["order_id"], op_column=op_column),
        [],
    )


class TestMergeStatement:
    def test_binds_the_temp_table_as_source_and_the_target_as_target(self):
        sql = _merge_sql()
        assert sql.startswith("MERGE INTO `main`.`analytics`.`orders` AS target ")
        assert "USING `main`.`analytics`.`orders_ab12cd34` AS source " in sql

    def test_matches_on_the_key_columns(self):
        assert "ON target.`order_id` = source.`order_id` " in _merge_sql()

    def test_joins_a_composite_key_with_and(self):
        sql = _merge_sql(on=["db", "order_id"])
        assert (
            "ON target.`db` = source.`db` AND target.`order_id` = source.`order_id` "
            in sql
        )

    def test_gates_every_clause_on_the_marker(self):
        sql = _merge_sql()
        assert "WHEN MATCHED AND (LOWER(source.`op`) = 'd') THEN DELETE" in sql
        assert "WHEN MATCHED AND (LOWER(source.`op`) = 'u') THEN UPDATE SET" in sql
        assert (
            "WHEN NOT MATCHED BY TARGET AND (LOWER(source.`op`) = 'i') THEN INSERT"
            in sql
        )

    def test_deletes_before_it_updates(self):
        sql = _merge_sql()
        assert sql.index("THEN DELETE") < sql.index("THEN UPDATE")

    def test_inserts_by_target_not_by_source(self):
        """``BY TARGET`` is the row-absent-from-the-target case; ``BY SOURCE``
        would be the opposite and would delete rows this batch never mentioned."""
        assert "WHEN NOT MATCHED BY TARGET" in _merge_sql()
        assert "BY SOURCE" not in _merge_sql()

    def test_writes_the_bare_wildcard_on_strict(self):
        """With evolution off, ``*`` covers the target's own columns and the
        marker is not among them, so there is nothing to exclude."""
        sql = _merge_sql(schema_evolution="strict")
        assert "THEN UPDATE SET *" in sql
        assert "THEN INSERT *" in sql
        assert "EXCEPT" not in sql

    def test_excludes_the_marker_from_the_wildcard_on_update(self):
        """Evolution widens ``*`` to columns the source has and the target does
        not -- the marker is exactly that, so it has to be excluded or it lands
        on the target as a new column."""
        sql = _merge_sql(schema_evolution="update")
        assert "THEN UPDATE SET * EXCEPT (`op`)" in sql
        assert "THEN INSERT * EXCEPT (`op`)" in sql

    def test_excludes_the_folded_marker(self):
        sql = _merge_sql(op_column="`OP`", schema_evolution="update")
        assert "EXCEPT (`op`)" in sql

    def test_the_wildcard_is_never_an_explicit_column_list(self):
        for schema_evolution in ("strict", "update"):
            sql = _merge_sql(schema_evolution=schema_evolution)
            assert "SET *," not in sql and "INSERT *," not in sql

    def test_evolves_the_schema_on_update(self):
        sql = _merge_sql(schema_evolution="update")
        assert sql.startswith("MERGE WITH SCHEMA EVOLUTION INTO ")

    def test_does_not_evolve_the_schema_on_strict(self):
        sql = _merge_sql(schema_evolution="strict")
        assert sql.startswith("MERGE INTO ")
        assert "SCHEMA EVOLUTION" not in sql

    def test_lower_cases_the_marker_before_comparing(self):
        """A feed emitting 'I' / 'U' / 'D' is claimed by the same clauses as one
        emitting lower case."""
        sql = _merge_sql()
        assert sql.count("LOWER(source.`op`)") == 3

    def test_folds_the_marker_and_key_names(self):
        sql = _merge_sql(on=["`OrderId`"], op_column="`OP`")
        assert "target.`orderid` = source.`orderid`" in sql
        assert "LOWER(source.`op`)" in sql

    def test_quotes_a_key_that_needs_it(self):
        assert "target.`weird col` = source.`weird col`" in _merge_sql(
            on=["`weird col`"]
        )

    def test_compares_against_the_bare_marker(self):
        """Interpolating a str-mixin enum member would emit
        ``'CdcOperation.DELETE'`` -- valid SQL that matches no row."""
        sql = _merge_sql()
        assert "CdcOperation" not in sql
        for operation in (
            CdcOperation.INSERT,
            CdcOperation.UPDATE,
            CdcOperation.DELETE,
        ):
            assert f"= '{operation.value}'" in sql


# ---------------------------------------------------------------------------
# Creating the merge target
#
# The marker is read off each row and never written, so a target the merge path
# brings into being must not carry it: nothing would ever set it, and every row
# would read NULL.
# ---------------------------------------------------------------------------


def _ensure_target(op_column: str = "op", schema_evolution: str = "update"):
    cursor = _RecordingCursor()
    columns = [("order_id", "BIGINT"), ("op", "STRING")]
    _dialect(cursor, schema_evolution=schema_evolution).ensure_target(
        _TARGET, _staged(columns), MergeCfg(on=["order_id"], op_column=op_column)._op_column()
    )
    return cursor.statements


class TestEnsureTarget:
    def test_creates_the_target_without_the_marker(self):
        create = _only_starting_with(_ensure_target(), "CREATE TABLE IF NOT EXISTS")
        assert create == (
            "CREATE TABLE IF NOT EXISTS `main`.`analytics`.`orders` "
            "USING DELTA AS SELECT * EXCEPT (`op`) "
            "FROM `main`.`analytics`.`orders_ab12cd34` LIMIT 0"
        )

    def test_leaves_an_existing_target_alone(self):
        """IF NOT EXISTS: a merge onto a target that already exists must not
        rebuild it from one batch's schema."""
        assert any(
            s.startswith("CREATE TABLE IF NOT EXISTS") for s in _ensure_target()
        )

    @pytest.mark.parametrize("op_column", ["op", "OP", "`opCol`"])
    def test_folds_the_marker_before_excluding_it(self, op_column):
        """Databricks resolves names case-insensitively, so the folded marker
        drops the batch's column whichever case the publisher wrote."""
        cursor = _RecordingCursor()
        columns = [("order_id", "BIGINT"), (op_column.strip("`"), "STRING")]
        _dialect(cursor).ensure_target(
            _TARGET, _staged(columns), op_column.strip("`").lower()
        )
        create = _only_starting_with(cursor.statements, "CREATE TABLE IF NOT EXISTS")
        assert f"EXCEPT (`{op_column.strip('`')}`)" in create

    @pytest.mark.parametrize(
        "schema_evolution,widening", [("update", "true"), ("strict", "false")]
    )
    def test_marks_the_target_for_type_widening(self, schema_evolution, widening):
        """The mark is set on every merge, not only when evolving: a target an
        earlier update-mode run marked would otherwise go on widening after a
        run asked for strict."""
        alter = _only_starting_with(
            _ensure_target(schema_evolution=schema_evolution), "ALTER TABLE"
        )
        assert alter == (
            "ALTER TABLE `main`.`analytics`.`orders` "
            f"SET TBLPROPERTIES ('delta.enableTypeWidening' = '{widening}')"
        )

    def test_creates_the_target_before_marking_it(self):
        # The mark is an ALTER, so the table has to exist first.
        statements = _ensure_target()
        create = _index_starting_with(statements, "CREATE TABLE IF NOT EXISTS")
        alter = _index_starting_with(statements, "ALTER TABLE")
        assert create < alter


# ---------------------------------------------------------------------------
# Staging through the volume
# ---------------------------------------------------------------------------


def _staging_cursor() -> _RecordingCursor:
    return _RecordingCursor([("order_id", "BIGINT"), ("op", "STRING")])


class TestStaging:
    def test_uploads_builds_and_loads_in_that_order(self, tmp_path):
        batch = tmp_path / "in.parquet"
        batch.write_bytes(b"")
        cursor, ws = _staging_cursor(), _FakeWorkspaceClient()
        with _dialect(cursor, ws_client=ws).staging(_TARGET, batch) as staged:
            assert staged.columns == [("order_id", "BIGINT"), ("op", "STRING")]
        assert len(ws.uploaded) == 1
        statements = cursor.statements
        create = _index_starting_with(statements, "CREATE TABLE `main`")
        copy = _index_starting_with(statements, "COPY INTO")
        assert create < copy

    def test_keeps_the_marker_in_the_scratch_table(self, tmp_path):
        """The clauses read the marker off the scratch table, so only the target
        loses it."""
        batch = tmp_path / "in.parquet"
        batch.write_bytes(b"")
        cursor = _staging_cursor()
        with _dialect(cursor).staging(_TARGET, batch):
            pass
        create = _only_starting_with(cursor.statements, "CREATE TABLE `main`")
        assert "EXCEPT" not in create
        assert "SELECT * FROM read_files" in create

    def test_loads_the_scratch_table_not_the_target(self, tmp_path):
        batch = tmp_path / "in.parquet"
        batch.write_bytes(b"")
        cursor = _staging_cursor()
        with _dialect(cursor).staging(_TARGET, batch):
            pass
        copy = _only_starting_with(cursor.statements, "COPY INTO")
        assert copy.startswith("COPY INTO `main`.`analytics`.`orders_")
        assert not copy.startswith("COPY INTO `main`.`analytics`.`orders` ")

    def test_drops_the_scratch_table_and_the_staged_file_on_the_way_out(
        self, tmp_path
    ):
        """Both outlive the statement -- a real catalog table and a file on the
        volume -- so both are cleaned up here."""
        batch = tmp_path / "in.parquet"
        batch.write_bytes(b"")
        cursor, ws = _staging_cursor(), _FakeWorkspaceClient()
        with _dialect(cursor, ws_client=ws).staging(_TARGET, batch) as staged:
            relation = staged.relation
        assert f"DROP TABLE IF EXISTS {relation}" in cursor.statements
        assert ws.deleted == ws.uploaded

    def test_cleans_up_when_the_merge_fails(self, tmp_path):
        batch = tmp_path / "in.parquet"
        batch.write_bytes(b"")
        cursor, ws = _staging_cursor(), _FakeWorkspaceClient()
        with pytest.raises(RuntimeError):
            with _dialect(cursor, ws_client=ws).staging(_TARGET, batch) as staged:
                relation = staged.relation
                raise RuntimeError("merge blew up")
        assert f"DROP TABLE IF EXISTS {relation}" in cursor.statements
        assert ws.deleted == ws.uploaded


# ---------------------------------------------------------------------------
# Schema evolution happens inside the statement
# ---------------------------------------------------------------------------


class TestEvolve:
    def test_alters_nothing_and_reports_nothing(self):
        """``WITH SCHEMA EVOLUTION`` adds the columns and
        ``delta.enableTypeWidening`` widens the types, both as the statement
        runs -- so there is no ALTER pass and nothing to report back."""
        cursor = _RecordingCursor()
        evolved = _dialect(cursor).evolve(
            _TARGET, _staged([("order_id", "BIGINT")]), "op"
        )
        assert evolved == []
        assert cursor.statements == []


# ---------------------------------------------------------------------------
# The shared driver, read through this dialect
# ---------------------------------------------------------------------------


class TestWriteColumns:
    def test_keeps_the_batch_order_less_the_marker(self):
        columns = [("order_id", "BIGINT"), ("status", "STRING"), ("op", "STRING")]
        cursor = _RecordingCursor(columns)
        written = write_columns(
            _dialect(cursor), _TARGET, _staged(columns), _cfg()
        )
        assert written == ["order_id", "status"]

    def test_matches_case_insensitively(self):
        cursor = _RecordingCursor([("ORDER_ID", "BIGINT"), ("OP", "STRING")])
        columns = [("order_id", "BIGINT"), ("op", "STRING")]
        written = write_columns(
            _dialect(cursor), _TARGET, _staged(columns), _cfg()
        )
        assert written == ["order_id"]

    def test_refuses_a_marker_the_batch_lacks(self):
        columns = [("order_id", "BIGINT")]
        cursor = _RecordingCursor(columns)
        with pytest.raises(DatabricksRuntimeException) as ei:
            write_columns(_dialect(cursor), _TARGET, _staged(columns), _cfg())
        assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_19


def _batch(path, **columns):
    """A parquet holding whatever columns the test names."""
    import polars as pl

    pl.DataFrame(columns).write_parquet(path)
    return path


def _coalesce(batch: Path, cfg: MergeCfg):
    return coalesced_batch(_dialect(_RecordingCursor()), "t", batch, cfg)


class TestIncomingColumn:
    def test_finds_a_column_whatever_case_it_was_written_in(self):
        dialect = _dialect(_RecordingCursor())
        names = ["OrderId", "op"]
        assert incoming_column(dialect, names, "orderid", "t") == "OrderId"

    def test_is_none_when_absent(self):
        dialect = _dialect(_RecordingCursor())
        assert incoming_column(dialect, ["a", "b"], "c", "t") is None

    def test_rejects_an_ambiguous_match(self):
        """Databricks resolves names case-insensitively, so two columns differing
        only in case cannot be told apart -- picking one would merge on the wrong
        data."""
        dialect = _dialect(_RecordingCursor())
        with pytest.raises(DatabricksRuntimeException) as ei:
            incoming_column(dialect, ["op", "OP"], "op", "main.analytics.orders")
        assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_21
        assert "'op'" in str(ei.value) and "'OP'" in str(ei.value)


class TestCoalescedBatch:
    def test_reduces_each_key_to_one_row(self, tmp_path):
        import polars as pl

        batch = _batch(
            tmp_path / "in.parquet",
            order_id=[1, 1, 1, 2],
            op=["i", "u", "u", "u"],
            v=[10, 20, 30, 40],
        )
        with _coalesce(batch, _cfg()) as out:
            # Key 1's three records collapse to one insert with its latest payload.
            assert sorted(pl.read_parquet(out).rows()) == [(1, "i", 30), (2, "u", 40)]

    def test_keeps_the_original_basename(self, tmp_path):
        batch = _batch(tmp_path / "in.parquet", k=[1], op=["i"])
        with _coalesce(batch, _cfg(on=["k"])) as out:
            assert out.name == "in.parquet"
            assert out != batch

    def test_removes_its_temp_directory(self, tmp_path):
        batch = _batch(tmp_path / "in.parquet", k=[1], op=["i"])
        with _coalesce(batch, _cfg(on=["k"])) as out:
            assert out.exists()
        assert not out.exists() and not out.parent.exists()

    def test_leaves_the_original_alone(self, tmp_path):
        import polars as pl

        batch = _batch(tmp_path / "in.parquet", k=[1, 1], op=["i", "u"], v=[1, 2])
        with _coalesce(batch, _cfg(on=["k"])):
            pass
        assert pl.read_parquet(batch).height == 2

    def test_resolves_a_differently_cased_column(self, tmp_path):
        """The cfg folds to lower case but the publisher wrote upper, and
        Databricks would resolve them to one column -- so the coalesce has to
        find it too."""
        import polars as pl

        batch = _batch(
            tmp_path / "in.parquet", ORDER_ID=[7, 7], OP=["i", "u"], v=[1, 2]
        )
        with _coalesce(batch, _cfg()) as out:
            assert pl.read_parquet(out).rows() == [(7, "i", 2)]

    def test_does_not_match_a_name_differing_by_more_than_case(self, tmp_path):
        """``OrderId`` and ``order_id`` are two columns to Databricks as well, so
        neither stands in for the other."""
        batch = _batch(tmp_path / "in.parquet", OrderId=[7], op=["i"])
        with pytest.raises(DatabricksRuntimeException) as ei:
            with _coalesce(batch, _cfg()):
                pass
        assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_20

    def test_rejects_a_missing_marker(self, tmp_path):
        batch = _batch(tmp_path / "in.parquet", k=[1], v=[10])
        with pytest.raises(DatabricksRuntimeException) as ei:
            with _coalesce(batch, _cfg(on=["k"])):
                pass
        assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_19

    def test_rejects_a_missing_key(self, tmp_path):
        batch = _batch(tmp_path / "in.parquet", k=[1], op=["i"])
        with pytest.raises(DatabricksRuntimeException) as ei:
            with _coalesce(batch, _cfg(on=["nope"])):
                pass
        assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_20
        assert "'nope'" in str(ei.value)

    def test_drops_an_ephemeral_key(self, tmp_path):
        """A key inserted and deleted inside one batch never reached the target,
        so it reaches neither the upload nor the merge."""
        import polars as pl

        batch = _batch(tmp_path / "in.parquet", k=[1, 1], op=["i", "d"], v=[1, 2])
        with _coalesce(batch, _cfg(on=["k"])) as out:
            got = pl.read_parquet(out)
        assert got.is_empty()
