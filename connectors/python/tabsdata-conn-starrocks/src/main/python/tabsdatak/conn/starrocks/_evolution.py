#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import re
import time
from decimal import Decimal

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema.delta import (
    Action,
    DialectRenderer,
    SchemaChange,
    TypeWideningMap,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema.evolution import evolve_with_iceberg

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema.policy import STRICT, SchemaEvolution
from tabsdatak.conn.starrocks.error import StarRocksErrorCode

logger = logging.getLogger(__name__)

_INTEGERS = ("tinyint", "smallint", "int", "bigint", "largeint")
_INTEGER_BITS = dict(zip(_INTEGERS, (8, 16, 32, 64, 128)))
_TEXT = ("char", "varchar", "string")

TYPE_WIDENING_MAP: TypeWideningMap = {
    **{wider: set(_INTEGERS[:i]) for i, wider in enumerate(_INTEGERS) if i},
    "double": {"float"},
}

_TYPE = re.compile(r"^\s*(\w+)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?\s*$")

ALTER_POLL_SECONDS = 1.0
ALTER_TIMEOUT_SECONDS = 1800.0


def normalized(type_: str) -> str:
    """``type_`` as ``DESC FILES`` spells it: lower case, and an integer
    without the display width ``information_schema`` adds to it."""
    lowered = type_.strip().lower()
    match = _TYPE.match(lowered)
    if match and match[1] in _INTEGERS:
        return match[1]
    return lowered


def _parsed(type_: str) -> tuple[str, int | None, int]:
    match = _TYPE.match(type_.strip().lower())
    if not match:
        return type_.strip().lower(), None, 0
    return match[1], int(match[2]) if match[2] else None, int(match[3] or 0)


def same_base(old: str, new: str) -> bool:
    return _parsed(old)[0] == _parsed(new)[0]


def can_widen(old: str, new: str) -> bool:
    old_base, old_p, old_s = _parsed(old)
    new_base, new_p, new_s = _parsed(new)
    if old_base != new_base or old_p is None or new_p is None:
        return False
    if old_base == "varchar":
        return new_p > old_p
    if old_base == "decimal":
        return new_s >= old_s and new_p - new_s >= old_p - old_s
    return False


def _narrower(stored: str, batch: str) -> bool:
    """Whether a ``stored`` integer or decimal column is narrower than the
    ``batch`` type of the same kind."""
    stored_base, batch_base = _parsed(stored)[0], _parsed(batch)[0]
    if stored_base in TYPE_WIDENING_MAP.get(batch_base, ()):
        return stored_base in _INTEGERS
    return stored_base == batch_base == "decimal" and can_widen(stored, batch)


def _holds(stored: str, low, high) -> bool:
    """Whether a ``stored`` integer or decimal column holds every value from
    ``low`` to ``high`` (``None`` when the batch has none)."""
    if low is None:
        return True
    base, precision, scale = _parsed(stored)
    if base in _INTEGER_BITS:
        half = 2 ** (_INTEGER_BITS[base] - 1)
        return -half <= low and high < half
    limit = Decimal(10) ** ((precision or 0) - scale)
    return -limit < Decimal(str(low)) and Decimal(str(high)) < limit


def _quote(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _unquoted(name: str) -> str:
    if len(name) > 1 and name.startswith("`") and name.endswith("`"):
        return name[1:-1].replace("``", "`")
    return name


class StarRocksRenderer(DialectRenderer):
    """``ALTER`` statements for StarRocks. A ``MODIFY COLUMN`` restates what
    the column already is besides its type -- a key column says ``KEY``, and
    the nullability stays -- or StarRocks reads the omission as a change it
    refuses."""

    SUPPORTED_ACTIONS = {Action.ADD_COLUMN, Action.ALTER_TYPE}

    def __init__(self, evolver: StarRocksSchemaEvolver) -> None:
        self._evolver = evolver

    def column_def(self, name, type_, nullable, default) -> str:
        return f"{_quote(name)} {type_} NULL"

    def render_alter(self, table_name: str, change: SchemaChange) -> str | None:
        column = _quote(change.column)
        if change.action is Action.ADD_COLUMN:
            type_ = change.detail["type"]
            return f"ALTER TABLE {table_name} ADD COLUMN {column} {type_} NULL"
        if change.action is Action.ALTER_TYPE:
            folded = change.column.lower()
            type_ = change.detail["new_type"]
            key = " KEY" if folded in self._evolver.keys else ""
            null = " NOT NULL" if folded in self._evolver.required else " NULL"
            statement = (
                f"ALTER TABLE {table_name} MODIFY COLUMN {column} {type_}{key}{null}"
            )
            self._evolver.retypes[statement] = (folded, normalized(type_))
            return statement
        return None


class StarRocksSchemaEvolver:
    """The three operations the Iceberg evolution pass makes against
    StarRocks, over one SQLAlchemy connection.

    StarRocks runs a column type change as an asynchronous job, one at a time
    per table -- or, on a table with fast schema evolution, at once and with no
    job at all -- so :meth:`execute` waits after a ``MODIFY COLUMN`` until the
    column reports its new type and no job for it is left running, before the
    next statement or the load runs.
    """

    def __init__(self, conn, database: str, table: str) -> None:
        self._conn = conn
        self._database = _unquoted(database)
        self._table = _unquoted(table)
        self.keys: set[str] = set()
        self.required: set[str] = set()
        self.retypes: dict[str, tuple[str, str]] = {}

    def fold(self, name: str) -> str:
        return name.lower()

    def target_columns(self, target: str) -> list[tuple[str, str]]:
        from sqlalchemy import text

        rows = self._conn.execute(
            text(
                "SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_KEY, IS_NULLABLE"
                " FROM information_schema.columns"
                " WHERE TABLE_SCHEMA = :db AND TABLE_NAME = :tbl"
                " ORDER BY ORDINAL_POSITION"
            ),
            {"db": self._database, "tbl": self._table},
        ).fetchall()
        self.keys = {name.lower() for name, _, key, _ in rows if key}
        self.required = {name.lower() for name, _, _, null in rows if null == "NO"}
        return [(name, normalized(type_)) for name, type_, _, _ in rows]

    def execute(self, statement: str) -> None:
        from sqlalchemy import text

        before = self._latest_job()
        self._conn.execute(text(statement))
        if statement in self.retypes:
            self._wait(before, *self.retypes[statement])

    def evolve(
        self, target: str, files_fn: str, evolution: SchemaEvolution
    ) -> list[str]:
        """``ALTER`` ``target`` to meet the staged file as far as ``evolution``
        allows, returning what changed. The file's types are the ones
        StarRocks itself reads from it (``DESC FILES``).

        Under ``strict`` no type difference is left to StarRocks unchecked: it
        refuses a text value its column cannot hold, but loads a number its
        column cannot hold as NULL. So a text column takes any text, and a
        narrower number column takes the batch only when every value fits
        (:meth:`_fitting`); any other difference is refused.
        """
        from sqlalchemy import text

        rows = self._conn.execute(text(f"DESC {files_fn}")).fetchall()
        columns = [(row[0], normalized(row[1])) for row in rows]
        if evolution == STRICT:
            columns = self._as_stored(target, files_fn, columns)
        return evolve_with_iceberg(
            self,
            target,
            columns,
            renderer=StarRocksRenderer(self),
            widening_map=TYPE_WIDENING_MAP,
            can_safely_widen=can_widen,
            same_base=same_base if evolution != STRICT else None,
            evolution=evolution,
        )

    def _as_stored(
        self, target: str, files_fn: str, columns: list[tuple[str, str]]
    ) -> list[tuple[str, str]]:
        """``columns`` with the stored type in place of each batch type the
        stored column takes as it is."""
        stored = {name.lower(): type_ for name, type_ in self.target_columns(target)}
        narrower = {}
        aligned = []
        for name, type_ in columns:
            held = stored.get(name.lower())
            if held is not None and held != type_:
                if _parsed(held)[0] in _TEXT and _parsed(type_)[0] in _TEXT:
                    type_ = held
                elif _narrower(held, type_):
                    narrower[name] = held
            aligned.append((name, type_))
        fitting = self._fitting(files_fn, narrower) if narrower else set()
        return [(name, narrower[name] if name in fitting else t) for name, t in aligned]

    def _fitting(self, files_fn: str, narrower: dict[str, str]) -> set[str]:
        """The ``narrower`` columns whose stored type holds every value the
        staged file has for them, read in one pass over those columns."""
        from sqlalchemy import text

        names = list(narrower)
        bounds = ", ".join(
            f"MIN({_quote(name)}), MAX({_quote(name)})" for name in names
        )
        row = self._conn.execute(text(f"SELECT {bounds} FROM {files_fn}")).fetchall()[0]
        return {
            name
            for i, name in enumerate(names)
            if _holds(narrower[name], row[2 * i], row[2 * i + 1])
        }

    def _latest_job(self) -> tuple | None:
        from sqlalchemy import text

        rows = (
            self._conn
            .execute(
                text(
                    f"SHOW ALTER TABLE COLUMN FROM {_quote(self._database)}"
                    f" WHERE TableName = :tbl ORDER BY CreateTime DESC LIMIT 1"
                ),
                {"tbl": self._table},
            )
            .mappings()
            .fetchall()
        )
        return tuple(rows[0].items()) if rows else None

    def _column_type(self, column: str) -> str | None:
        for name, type_ in self.target_columns(self._table):
            if name.lower() == column:
                return type_
        return None

    def _wait(self, before: tuple | None, column: str, type_: str) -> None:
        deadline = time.monotonic() + ALTER_TIMEOUT_SECONDS
        previous = dict(before or ()).get("JobId")
        while True:
            latest = self._latest_job()
            job = dict(latest) if latest else {}
            started = latest is not None and job.get("JobId") != previous
            state = job.get("State") if started else None
            if state == "CANCELLED":
                raise StarRocksErrorCode.STARROCKS_12.exception(
                    table=self._table, state=state, msg=job.get("Msg")
                )
            if state in (None, "FINISHED") and self._column_type(column) == type_:
                return
            if time.monotonic() > deadline:
                raise StarRocksErrorCode.STARROCKS_12.exception(
                    table=self._table, state=job.get("State"), msg="timed out"
                )
            time.sleep(ALTER_POLL_SECONDS)
