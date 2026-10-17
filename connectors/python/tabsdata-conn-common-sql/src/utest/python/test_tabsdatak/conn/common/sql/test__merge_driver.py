#
#  Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the shared merge driver's handling of a batch that loads or
truncates the table.

Such a batch replaces the target's rows instead of merging onto them: the
driver detects the reset on the batch as captured, refuses it for a dialect
with no replace form before anything is staged, stages only what follows the
last reset, and runs :func:`render_replace` in place of :func:`render_merge`.
The dialect here is a fake that records what it is asked to do, so the
statements are checked as rendered, not as any engine would run them.
"""

from __future__ import annotations

import contextlib
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge import MergeCfg

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge._batch import batch_resets

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge._driver import merge_batch

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge._statement import render_replace

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge.dialect import (
    MergeDialect,
    MergeErrors,
    MergeSyntax,
    Staged,
)
from tabsdatak.conn.common.sql.error import (
    SqlCommonErrorCode,
    SqlCommonRuntimeException,
)

_TARGET = "tgt"
_SCRATCH = "scratch"


class _Refused(Exception):
    pass


def _refusal(**kwargs) -> Exception:
    return _Refused(kwargs)


_ERRORS = MergeErrors(
    marker_absent=_refusal,
    keys_absent=_refusal,
    no_shared_columns=_refusal,
    ambiguous_column=_refusal,
)


class _FakeDialect(MergeDialect):
    def __init__(self, syntax: MergeSyntax, target_columns: list[str]) -> None:
        self._syntax = syntax
        self._target_columns = target_columns
        self.executed: list[str] = []
        self.staged_rows: list[tuple] | None = None

    @property
    def syntax(self) -> MergeSyntax:
        return self._syntax

    @property
    def errors(self) -> MergeErrors:
        return _ERRORS

    def quote_column(self, name: str) -> str:
        return f'"{name}"'

    def fold(self, name: str) -> str:
        return name

    @contextlib.contextmanager
    def staging(self, target, batch, watermark_cfg=None):
        frame = pl.read_parquet(batch)
        self.staged_rows = sorted(frame.rows(), key=str)
        yield Staged(
            relation=_SCRATCH,
            columns=[(name, str(dtype)) for name, dtype in frame.schema.items()],
        )

    def ensure_target(self, target, staged, marker) -> None:
        pass

    def target_columns(self, target):
        return [(name, "STRING") for name in self._target_columns]

    def execute(self, statement: str) -> None:
        self.executed.append(statement)


_MERGE = MergeSyntax(replace="merge", not_matched="WHEN NOT MATCHED BY TARGET")
_OVERWRITE = MergeSyntax(replace="overwrite")


def _cfg() -> MergeCfg:
    return MergeCfg(on=["pk"], op_column="op")


def _batch(tmp_path: Path, records: list[tuple]) -> Path:
    path = tmp_path / "batch.parquet"
    pl.DataFrame(
        {
            "pk": [rec[0] for rec in records],
            "op": [rec[1] for rec in records],
            "v": [rec[2] for rec in records],
        },
        schema={"pk": pl.String, "op": pl.String, "v": pl.Int64},
    ).write_parquet(path)
    return path


def _merge(tmp_path, records, syntax=_MERGE):
    dialect = _FakeDialect(syntax, ["pk", "v"])
    result = merge_batch(
        dialect,
        target=_TARGET,
        batch=_batch(tmp_path, records),
        cfg=_cfg(),
        schema_evolution="strict",
        ctx=SimpleNamespace(trx_id="trx"),
    )
    return dialect, result


def _replace_sql(syntax: MergeSyntax, columns=("pk", "v")) -> str:
    dialect = _FakeDialect(syntax, list(columns))
    staged = Staged(relation=_SCRATCH, columns=[(c, "STRING") for c in columns])
    return render_replace(dialect, _TARGET, staged, _cfg(), list(columns))


# --- render_replace --------------------------------------------------


class TestRenderReplace:
    def test_merge_form_matches_nothing(self):
        assert _replace_sql(_MERGE).startswith(
            f'MERGE INTO {_TARGET} AS target USING {_SCRATCH} AS source ON FALSE'
        )

    def test_merge_form_inserts_every_staged_row(self):
        assert (
            'WHEN NOT MATCHED BY TARGET THEN INSERT ("pk", "v")'
            ' VALUES (source."pk", source."v")'
        ) in _replace_sql(_MERGE)

    def test_merge_form_deletes_every_target_row_last(self):
        assert _replace_sql(_MERGE).endswith("WHEN NOT MATCHED BY SOURCE THEN DELETE")

    def test_merge_form_gates_nothing_on_the_marker(self):
        assert '"op"' not in _replace_sql(_MERGE)

    def test_merge_form_carries_the_evolution_modifier(self):
        syntax = replace(_MERGE, evolution_modifier=" WITH SCHEMA EVOLUTION")
        assert _replace_sql(syntax).startswith("MERGE WITH SCHEMA EVOLUTION INTO")

    def test_merge_form_writes_the_wildcard_less_the_marker(self):
        syntax = replace(_MERGE, wildcard_writes="* EXCEPT ({marker})")
        assert (
            'WHEN NOT MATCHED BY TARGET THEN INSERT * EXCEPT ("op")'
            in _replace_sql(syntax)
        )

    def test_overwrite_form(self):
        assert _replace_sql(_OVERWRITE) == (
            f'INSERT OVERWRITE INTO {_TARGET} ("pk", "v")'
            f' SELECT "pk", "v" FROM {_SCRATCH}'
        )


# --- batch_resets ----------------------------------------------------


class TestBatchResets:
    def test_reads_a_load(self, tmp_path):
        batch = _batch(tmp_path, [(None, "l", None), ("A", "i", 1)])
        assert batch_resets(_FakeDialect(_MERGE, []), _TARGET, batch, _cfg())

    def test_reads_a_truncate(self, tmp_path):
        batch = _batch(tmp_path, [("A", "i", 1), (None, "T", None)])
        assert batch_resets(_FakeDialect(_MERGE, []), _TARGET, batch, _cfg())

    def test_reads_no_reset_in_row_changes(self, tmp_path):
        batch = _batch(tmp_path, [("A", "i", 1), ("A", "d", 2)])
        assert not batch_resets(_FakeDialect(_MERGE, []), _TARGET, batch, _cfg())

    def test_refuses_a_batch_without_the_marker(self, tmp_path):
        batch = tmp_path / "batch.parquet"
        pl.DataFrame({"pk": ["A"], "v": [1]}).write_parquet(batch)
        with pytest.raises(_Refused):
            batch_resets(_FakeDialect(_MERGE, []), _TARGET, batch, _cfg())


# --- merge_batch -----------------------------------------------------


class TestMergeBatch:
    def test_a_batch_without_a_reset_merges(self, tmp_path):
        dialect, result = _merge(tmp_path, [("A", "i", 1), ("B", "u", 2)])
        assert not result.replaced
        assert len(dialect.executed) == 1
        assert "ON FALSE" not in dialect.executed[0]
        assert "WHEN MATCHED" in dialect.executed[0]

    def test_a_load_replaces(self, tmp_path):
        records = [("A", "u", 1), (None, "l", None), ("B", "i", 2), ("C", "i", 3)]
        dialect, result = _merge(tmp_path, records)
        assert result.replaced
        assert result.written == ["pk", "v"]
        assert dialect.executed == [_replace_sql(_MERGE)]

    def test_a_replace_stages_only_what_follows_the_last_reset(self, tmp_path):
        records = [
            ("A", "i", 1),
            (None, "t", None),
            ("B", "i", 2),
            ("B", "u", 3),
            ("C", "u", 4),
            ("D", "i", 5),
            ("D", "d", 6),
        ]
        dialect, _ = _merge(tmp_path, records)
        assert dialect.staged_rows == [("B", "i", 3), ("C", "i", 4)]

    def test_a_trailing_truncate_still_replaces_with_nothing(self, tmp_path):
        """Nothing is left to stage, but the statement still has to run: it is
        what empties the target."""
        dialect, result = _merge(tmp_path, [("A", "i", 1), (None, "t", None)])
        assert result.replaced
        assert dialect.staged_rows == []
        assert dialect.executed == [_replace_sql(_MERGE)]

    def test_the_overwrite_form_is_rendered_for_a_dialect_asking_for_it(
        self, tmp_path
    ):
        dialect, _ = _merge(tmp_path, [(None, "l", None), ("A", "i", 1)], _OVERWRITE)
        assert dialect.executed == [_replace_sql(_OVERWRITE)]

    def test_a_dialect_without_a_replace_form_refuses_before_staging(
        self, tmp_path
    ):
        dialect = _FakeDialect(MergeSyntax(), ["pk", "v"])
        with pytest.raises(SqlCommonRuntimeException) as ei:
            merge_batch(
                dialect,
                target=_TARGET,
                batch=_batch(tmp_path, [(None, "l", None), ("A", "i", 1)]),
                cfg=_cfg(),
                schema_evolution="strict",
                ctx=SimpleNamespace(trx_id="trx"),
            )
        assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_9
        assert _TARGET in str(ei.value)
        assert dialect.staged_rows is None
        assert dialect.executed == []

    def test_a_dialect_without_a_replace_form_still_merges(self, tmp_path):
        dialect, result = _merge(tmp_path, [("A", "i", 1)], MergeSyntax())
        assert not result.replaced
        assert len(dialect.executed) == 1
