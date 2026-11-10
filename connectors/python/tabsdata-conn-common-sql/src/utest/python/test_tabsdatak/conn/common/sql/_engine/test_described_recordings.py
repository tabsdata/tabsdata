#
# Copyright 2026 Tabsdata Inc.
#

"""Every column a live database described, replayed offline.

``resources/described`` holds, per database and driver, the cursor description
entry of one column per type, precision, scale and sign the database offers --
and of the expressions over them -- exactly as the driver returned it, with
what completes it: pymysql's field list, oracledb's entry attributes and the
type mysql-connector's database declares. Each must still be typed as the
reader typed it then, after its values were checked against the live data.
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import described

RECORDINGS = Path(__file__).parent / "resources" / "described"


def _cases() -> list[Any]:
    cases = []
    for path in sorted(RECORDINGS.glob("*.json")):
        recording = json.loads(path.read_text(encoding="utf-8"))
        for case in recording["cases"]:
            cases.append(
                pytest.param(
                    recording["driver"], case, id=f"{path.stem}-{case['id']}"
                )
            )
    return cases


def _code(code: dict) -> Any:
    [(kind, value)] = code.items()
    if kind in ("int", "str", "none"):
        return value
    if kind == "class":
        module, _, name = value.rpartition(".")
        return getattr(importlib.import_module(module), name)
    if kind == "oracle":
        import oracledb

        return getattr(oracledb, value)
    if kind == "db2":
        import ibm_db_dbi

        return ibm_db_dbi.DBAPITypeObject(tuple(value))
    raise ValueError(code)


class _Entry(tuple):
    """A description entry that carries attributes, as oracledb's do."""

    is_json = False
    vector_format = None


class _Connection:
    """Answers the temporary table mysql-connector's database is asked to
    describe with the type it declared."""

    def __init__(self, declared: str | None):
        self.declared = declared

    def execute(self, statement, parameters=None):
        if "SHOW COLUMNS" in str(statement):
            if self.declared is None:
                raise RuntimeError("the database refused to describe the query")
            return [("c", self.declared)]
        return None


def _replayed(case: dict) -> tuple[list[Any], described.Query]:
    name, code, *rest = case["entry"]
    entry = _Entry((name, _code(code), *rest))
    for attribute, value in (case.get("info") or {}).items():
        setattr(entry, attribute, value)
    fields = case.get("fields")
    cursor = SimpleNamespace(
        _result=SimpleNamespace(fields=[SimpleNamespace(**fields)] if fields else [])
    )
    query = described.Query(_Connection(case.get("declared")), cursor, "SELECT c", {})
    return [entry], query


@pytest.mark.parametrize("driver, case", _cases())
def test_a_recorded_column_is_typed_as_it_was_read(driver, case):
    description, query = _replayed(case)
    dtype = described.dtypes(driver, description, query).get(case["entry"][0])
    assert (None if dtype is None else str(dtype)) == case["dtype"]


def test_every_database_and_driver_is_recorded():
    recorded = {path.stem for path in RECORDINGS.glob("*.json")}
    assert recorded == {
        "db2-11.5_ibm_db_sa",
        "mariadb-11.4_mysqlconnector",
        "mariadb-11.4_pymysql",
        "mysql-8.4_mysqlconnector",
        "mysql-8.4_pymysql",
        "oracle-23_oracledb",
        "oracle-23_oracledb_assm",
        "postgres-18_pg8000",
        "postgres-18_psycopg2",
        "sqlserver-2022_pyodbc",
    }
