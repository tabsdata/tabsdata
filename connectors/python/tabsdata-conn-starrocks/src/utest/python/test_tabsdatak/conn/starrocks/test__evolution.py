#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re
from decimal import Decimal

import polars as pl
import pyarrow.parquet as pq
import pytest

from tabsdatak.conn.common.sql.error import SqlCommonRuntimeException

# noinspection PyProtectedMember
from tabsdatak.conn.starrocks import _evolution
from tabsdatak.conn.starrocks.error import StarRocksErrorCode, StarRocksRuntimeException

pytestmark = pytest.mark.starrocks

_TARGET = "`db`.`t`"


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def mappings(self):
        return self


class _Conn:
    """Answers the evolver's three reads and records every statement. A
    ``MODIFY COLUMN`` retypes the column, and starts the next of ``jobs``
    (``None`` for a change made at once, with no job)."""

    def __init__(self, table, batch, jobs=(None,)):
        self._table = [
            (*column, "", "YES") if len(column) == 2 else column for column in table
        ]
        self._batch = batch
        self._jobs = list(jobs)
        self.statements: list[str] = []
        self.scans: list[str] = []
        self.job: dict | None = None

    def execute(self, statement, params=None):
        sql = str(statement)
        if "information_schema.columns" in sql:
            return _Result(self._table)
        if sql.startswith("SELECT"):
            self.scans.append(sql)
            return _Result([])
        if sql.startswith("DESC "):
            return _Result([(name, type_, "YES") for name, type_ in self._batch])
        if sql.startswith("SHOW ALTER TABLE COLUMN"):
            return _Result([self.job] if self.job else [])
        self.statements.append(sql)
        modify = re.search(r"MODIFY COLUMN `(\w+)` (\S+)", sql)
        if modify:
            job = self._jobs.pop(0)
            self.job = job or self.job
            if not job or job["State"] == "FINISHED":
                self._table = [
                    (n, modify[2], k, null) if n == modify[1] else (n, t, k, null)
                    for n, t, k, null in self._table
                ]
        return _Result([])


_POLARS = {"bigint": pl.Int64, "decimal(20,2)": pl.Decimal(20, 2)}


def _footer(tmp_path, columns: dict[str, tuple[str, list]]):
    """The Parquet footer of a staged file holding ``columns``: name ->
    (batch type, values)."""
    path = tmp_path / "staged.parquet"
    pl.DataFrame(
        {
            name: [None if v is None else _value(type_, v) for v in values]
            for name, (type_, values) in columns.items()
        },
        schema={name: _POLARS[type_] for name, (type_, _) in columns.items()},
    ).write_parquet(path)
    return pq.read_metadata(path)


def _value(type_: str, value):
    return Decimal(value) if type_.startswith("decimal") else value


def _evolve(conn, evolution, footer=None):
    evolver = _evolution.StarRocksSchemaEvolver(conn, "db", "t")
    return evolver.evolve(_TARGET, "FILES(...)", evolution, footer)


@pytest.mark.parametrize(
    "stored,normal",
    [
        ("bigint(20)", "bigint"),
        ("INT(11)", "int"),
        ("varchar(255)", "varchar(255)"),
        ("decimal(10, 2)", "decimal(10,2)"),
        ("DECIMAL(38,9)", "decimal(38,9)"),
    ],
)
def test_a_type_is_spelled_as_desc_files_spells_it(stored, normal):
    assert _evolution.normalized(stored) == normal


@pytest.mark.parametrize(
    "old,new,widens",
    [
        ("varchar(255)", "varchar(1048576)", True),
        ("varchar(255)", "varchar(10)", False),
        ("decimal(10,2)", "decimal(12,2)", True),
        ("decimal(10,2)", "decimal(10,4)", False),
        ("int", "bigint", False),
    ],
)
def test_can_widen_a_length_or_a_decimal(old, new, widens):
    assert _evolution.can_widen(old, new) is widens


def test_update_adds_a_new_column():
    conn = _Conn(
        [("id", "bigint(20)")], [("id", "bigint"), ("note", "varchar(1048576)")]
    )
    assert _evolve(conn, "update") == ["note"]
    assert conn.statements == [
        "ALTER TABLE `db`.`t` ADD COLUMN `note` varchar(1048576) NULL"
    ]


def test_update_widens_and_waits_for_the_job(monkeypatch):
    monkeypatch.setattr(_evolution, "ALTER_POLL_SECONDS", 0)
    conn = _Conn(
        [("id", "bigint(20)"), ("n", "int(11)")],
        [("id", "bigint"), ("n", "bigint")],
        jobs=[{"JobId": 7, "State": "FINISHED"}],
    )
    assert _evolve(conn, "update") == ["n"]
    assert conn.statements == ["ALTER TABLE `db`.`t` MODIFY COLUMN `n` bigint NULL"]


def test_a_cancelled_type_change_fails_the_write(monkeypatch):
    monkeypatch.setattr(_evolution, "ALTER_POLL_SECONDS", 0)
    conn = _Conn(
        [("n", "int(11)")],
        [("n", "bigint")],
        jobs=[{"JobId": 7, "State": "CANCELLED", "Msg": "no"}],
    )
    with pytest.raises(StarRocksRuntimeException) as raised:
        _evolve(conn, "update")
    assert raised.value.api_error is StarRocksErrorCode.STARROCKS_12


def test_strict_refuses_a_new_column():
    conn = _Conn(
        [("id", "bigint(20)")], [("id", "bigint"), ("note", "varchar(1048576)")]
    )
    with pytest.raises(SqlCommonRuntimeException):
        _evolve(conn, "strict")
    assert conn.statements == []


@pytest.mark.parametrize("evolution", ["strict", "update"])
def test_a_lacking_column_and_a_bounded_varchar_change_nothing_on_strict(
    monkeypatch, evolution
):
    monkeypatch.setattr(_evolution, "ALTER_POLL_SECONDS", 0)
    conn = _Conn(
        [("id", "bigint(20)"), ("s", "varchar(255)"), ("x", "int(11)")],
        [("id", "bigint"), ("s", "varchar(1048576)")],
        jobs=[{"JobId": 7, "State": "FINISHED"}],
    )
    if evolution == "strict":
        assert _evolve(conn, evolution) == []
        assert conn.statements == []
    else:
        assert _evolve(conn, evolution) == ["s"]


@pytest.mark.parametrize(
    "stored,batch",
    [("char(3)", "varchar(1048576)"), ("varchar(10)", "string"), ("string", "char(2)")],
)
def test_strict_leaves_a_text_length_to_starrocks(stored, batch):
    """StarRocks refuses a text value its column cannot hold."""
    conn = _Conn([("c", stored)], [("c", batch)])
    assert _evolve(conn, "strict") == []
    assert conn.statements == [] and conn.scans == []


@pytest.mark.parametrize(
    "stored,batch,low,high",
    [
        ("int(11)", "bigint", -(2**31), 2**31 - 1),
        ("smallint(6)", "bigint", None, None),
        ("decimal(10,2)", "decimal(20,2)", "-99999999.99", "99999999.99"),
    ],
)
def test_strict_takes_numbers_their_narrower_column_holds(
    tmp_path, stored, batch, low, high
):
    """Read off the staged file's footer: no query reads the data."""
    conn = _Conn([("n", stored)], [("n", batch)])
    footer = _footer(tmp_path, {"n": (batch, [low, high])})
    assert _evolve(conn, "strict", footer) == []
    assert conn.statements == [] and conn.scans == []


@pytest.mark.parametrize(
    "stored,batch,low,high",
    [
        ("int(11)", "bigint", 1, 2**40),
        ("int(11)", "bigint", -(2**31) - 1, 0),
        ("decimal(10,2)", "decimal(20,2)", "0", "100000000.00"),
    ],
)
def test_strict_refuses_a_number_its_narrower_column_cannot_hold(
    tmp_path, stored, batch, low, high
):
    """StarRocks would load it as NULL, not refuse it."""
    conn = _Conn([("n", stored)], [("n", batch)])
    footer = _footer(tmp_path, {"n": (batch, [low, high])})
    with pytest.raises(SqlCommonRuntimeException, match="SQL_COMMON_10"):
        _evolve(conn, "strict", footer)
    assert conn.statements == [] and conn.scans == []


def test_strict_without_a_footer_takes_no_narrower_number():
    conn = _Conn([("n", "int(11)")], [("n", "bigint")])
    with pytest.raises(SqlCommonRuntimeException, match="SQL_COMMON_10"):
        _evolve(conn, "strict")


def test_strict_takes_a_decimal_spelled_with_a_space_as_its_own_type():
    conn = _Conn([("d", "decimal(10, 2)")], [("d", "decimal(10,2)")])
    assert _evolve(conn, "strict") == []


@pytest.mark.parametrize("batch", ["decimal(20,4)", "decimal(10,4)"])
def test_strict_refuses_a_decimal_that_would_lose_fractional_digits(batch):
    """More fractional digits than the column keeps would be rounded."""
    conn = _Conn([("n", "decimal(10,2)")], [("n", batch)])
    with pytest.raises(SqlCommonRuntimeException, match="SQL_COMMON_10"):
        _evolve(conn, "strict")


def test_strict_refuses_a_number_of_another_kind_without_a_scan():
    conn = _Conn([("d", "decimal(10,2)")], [("d", "double")])
    with pytest.raises(SqlCommonRuntimeException, match="SQL_COMMON_10"):
        _evolve(conn, "strict")
    assert conn.scans == []


def test_strict_takes_every_narrower_column_from_one_footer(tmp_path):
    conn = _Conn(
        [("a", "int(11)"), ("b", "smallint(6)"), ("s", "char(2)")],
        [("a", "bigint"), ("b", "bigint"), ("s", "varchar(1048576)")],
    )
    footer = _footer(tmp_path, {"a": ("bigint", [1, 2]), "b": ("bigint", [3, 4])})
    assert _evolve(conn, "strict", footer) == []
    assert conn.scans == []


def test_a_key_column_keeps_its_key_and_nullability(monkeypatch):
    monkeypatch.setattr(_evolution, "ALTER_POLL_SECONDS", 0)
    conn = _Conn(
        [("id", "bigint(20)", "DUP", "NO"), ("name", "varchar(255)", "DUP", "YES")],
        [("id", "bigint"), ("name", "varchar(1048576)")],
        jobs=[{"JobId": 7, "State": "FINISHED"}],
    )
    _evolve(conn, "update")
    assert conn.statements == [
        "ALTER TABLE `db`.`t` MODIFY COLUMN `name` varchar(1048576) KEY NULL"
    ]


def test_a_quoted_table_name_is_looked_up_unquoted():
    conn = _Conn([("id", "bigint(20)")], [("id", "bigint")])
    evolver = _evolution.StarRocksSchemaEvolver(conn, "`db`", "`t``x`")
    assert (evolver._database, evolver._table) == ("db", "t`x")


def test_a_type_change_made_at_once_needs_no_job(monkeypatch):
    monkeypatch.setattr(_evolution, "ALTER_POLL_SECONDS", 0)
    conn = _Conn([("s", "varchar(255)")], [("s", "varchar(1048576)")], jobs=[None])
    assert _evolve(conn, "update") == ["s"]
