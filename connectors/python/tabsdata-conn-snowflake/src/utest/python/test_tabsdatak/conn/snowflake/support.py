#
# Copyright 2026 Tabsdata Inc.
#

"""Fakes and statement readers shared by the write-path and merge-dialect tests.

The write path reads as well as writes, so a test that drives it has to answer
the reads: a target's columns out of ``DESC TABLE`` and a staged file's schema
out of ``infer_schema``. Both come back through ``fetchall`` against a
``description`` naming the output's own columns, which is what these fakes
model.
"""

from __future__ import annotations

import re

# The two reads the write path makes, keyed by the prefix of the statement that
# issues them: a table's columns and types off ``DESC TABLE``, and the staged
# file's inferred schema.
_PROBE_PREFIX = "DESC TABLE"
_INFER_PREFIX = "SELECT COLUMN_NAME, TYPE FROM table (infer_schema"

_ADD_COLUMN_RE = re.compile(r'ADD COLUMN IF NOT EXISTS "((?:[^"]|"")*)" (.+)$')

# ``DESC TABLE`` reports more columns than the two the connector reads, and
# neither of them first, so the fake answers in that shape: a reader that goes
# by position instead of by name in ``description`` fails here.
_DESC_FIELDS = ("kind", "name", "null?", "type")
_NAME_AT = _DESC_FIELDS.index("name")


def _desc_row(
    column: str | tuple[str, str],
    staged: dict[str, str] | None = None,
) -> tuple[str, ...]:
    """A ``DESC TABLE`` row for a target column. A bare name takes the type the
    staged file reports for it, or ``TEXT`` when the file has no such column."""
    if isinstance(column, str):
        name, type_ = column, (staged or {}).get(column.upper(), "TEXT")
    else:
        name, type_ = column
    return ("COLUMN", name, "Y", type_)


class _FakeCursor:
    """Captures every executed statement, and answers the ones ``results``
    describes.

    The write path reads as well as writes -- a table's columns out of
    ``DESC TABLE``, and ``infer_schema`` rows, both off ``fetchall`` against a
    ``description`` naming the output's own columns -- so ``results`` maps a SQL
    prefix to the ``(description, rows)`` the driver would return for it. A
    statement matching no prefix answers empty, which is what the pure-DDL tests
    need.

    An ``ALTER TABLE ... ADD COLUMN`` grows the answer to the column probe, with
    the type the statement gave it, so a statement that reads the table after
    one sees the column it added. Without that the fake would report a schema
    its own DDL had already changed.
    """

    def __init__(
        self,
        log: list[str],
        results: dict[str, tuple[list[tuple], list[tuple]]] | None = None,
    ) -> None:
        self._log = log
        self._results = results if results is not None else {}
        self.description: list[tuple] = []
        self._rows: list[tuple] = []
        self.closed = False

    def execute(self, sql: str) -> None:
        self._log.append(sql)
        self._apply_add_column(sql)
        self.description, self._rows = [], []
        for prefix, (description, rows) in self._results.items():
            if sql.startswith(prefix):
                self.description, self._rows = description, rows
                return

    def _apply_add_column(self, sql: str) -> None:
        match = _ADD_COLUMN_RE.search(sql)
        probe = self._results.get(_PROBE_PREFIX)
        if match is None or probe is None:
            return
        description, rows = probe
        name = match.group(1).replace('""', '"')
        if all(row[_NAME_AT].upper() != name.upper() for row in rows):
            self._results[_PROBE_PREFIX] = (
                description,
                [*rows, _desc_row((name, match.group(2)))],
            )

    def fetchall(self) -> list[tuple]:
        return list(self._rows)

    def close(self) -> None:
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class _FakeConnection:
    def __init__(
        self,
        results: dict[str, tuple[list[tuple], list[tuple]]] | None = None,
    ) -> None:
        self.log: list[str] = []
        self.results = results or {}
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self.log, self.results)

    def close(self) -> None:
        self.closed = True


def column_fakes(
    table_columns: list[str | tuple[str, str]],
    file_columns: list[tuple[str, str]],
) -> _FakeConnection:
    """A fake whose target table has ``table_columns`` and whose staged file
    infers to ``file_columns``.

    A target column given as a ``(name, type)`` pair carries that type -- what a
    test about widening turns on. Given as a bare name it takes the type the
    staged file reports for it, so a test about names alone describes a target
    whose types have not moved.
    """
    staged = {name.upper(): type_ for name, type_ in file_columns}
    return _FakeConnection({
        _PROBE_PREFIX: (
            [(field,) for field in _DESC_FIELDS],
            [_desc_row(column, staged) for column in table_columns],
        ),
        _INFER_PREFIX: ([("COLUMN_NAME",), ("TYPE",)], list(file_columns)),
    })


def assignments(sql: str) -> str:
    """Just the ``UPDATE SET`` assignments of a MERGE -- the ON condition and
    the clause predicates mention the same columns, so a substring check over
    the whole statement proves nothing."""
    start = sql.index("THEN UPDATE SET ") + len("THEN UPDATE SET ")
    return sql[start : sql.index(" WHEN NOT MATCHED", start)]


def insert_lists(sql: str) -> tuple[list[str], list[str]]:
    """The ``INSERT`` column list of a MERGE and its ``VALUES`` list, split on
    commas."""
    head = sql.index("THEN INSERT (") + len("THEN INSERT (")
    columns = sql[head : sql.index(")", head)]
    values_at = sql.index("VALUES (", head) + len("VALUES (")
    values = sql[values_at : sql.rindex(")")]
    return (
        [c.strip() for c in columns.split(",")],
        [v.strip() for v in values.split(",")],
    )
