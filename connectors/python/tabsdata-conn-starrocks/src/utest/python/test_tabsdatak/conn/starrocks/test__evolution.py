#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re

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
        self.job: dict | None = None

    def execute(self, statement, params=None):
        sql = str(statement)
        if "information_schema.columns" in sql:
            return _Result(self._table)
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


def _evolve(conn, evolution):
    evolver = _evolution.StarRocksSchemaEvolver(conn, "db", "t")
    return evolver.evolve(_TARGET, "FILES(...)", evolution)


@pytest.mark.parametrize(
    "stored,normal",
    [("bigint(20)", "bigint"), ("INT(11)", "int"), ("varchar(255)", "varchar(255)")],
)
def test_an_integer_loses_its_display_width(stored, normal):
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
