#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from decimal import Decimal

import polars as pl
import pytest
from sqlalchemy import Engine, inspect, text

from testi_tabsdatak.conn.common.sql.schema.policy import APPEND, Policy

NARROW = {
    "oracle": "id NUMBER(19), n NUMBER(10), f BINARY_FLOAT, d NUMBER(10,2)",
    "mysql": "id BIGINT, n INTEGER, f FLOAT, d DECIMAL(10,2)",
}
NARROW_DEFAULT = "id BIGINT, n INTEGER, f REAL, d DECIMAL(10,2)"

BATCH_SCHEMA = {"id": pl.Int64, "n": pl.Int64, "f": pl.Float64, "d": pl.Decimal(20, 2)}
WIDE = {
    "id": [1],
    "n": [2**40],
    "f": [1e300],
    "d": [Decimal("123456789012345678.25")],
}
FITTING = {"id": [1], "n": [5], "f": [1.5], "d": [Decimal("1.25")]}
OVERFLOWING = {**FITTING, "n": [2**40]}


def _create(engine: Engine, table: str) -> None:
    columns = NARROW.get(engine.dialect.name, NARROW_DEFAULT)
    with engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {table} ({columns})"))


def _types(engine: Engine, table: str) -> dict[str, str]:
    with engine.connect() as conn:
        schema = None
        if engine.dialect.name == "ibm_db_sa":
            schema = conn.execute(text("VALUES CURRENT SCHEMA")).scalar().strip()
        return {
            column["name"].lower(): column["type"].compile(dialect=engine.dialect)
            for column in inspect(conn).get_columns(table, schema=schema)
        }


def _row(policy: Policy, table: str) -> dict:
    frame = policy.read(table)
    frame = frame.rename({name: name.lower() for name in frame.columns})
    return frame.select("n", "f", "d").row(0, named=True)


def update_widens_a_number_column(policy: Policy, table: str, engine: Engine) -> None:
    policy.drop(table)
    _create(engine, table)
    narrow = _types(engine, table)
    policy.write(table, pl.LazyFrame(WIDE, schema=BATCH_SCHEMA), APPEND, "update")
    assert _types(engine, table) != narrow
    row = _row(policy, table)
    assert int(row["n"]) == WIDE["n"][0]
    assert float(row["f"]) == WIDE["f"][0]
    assert Decimal(str(row["d"])) == WIDE["d"][0]


def strict_leaves_a_fitting_number_to_the_database(
    policy: Policy, table: str, engine: Engine
) -> None:
    policy.drop(table)
    _create(engine, table)
    narrow = _types(engine, table)
    policy.write(table, pl.LazyFrame(FITTING, schema=BATCH_SCHEMA), APPEND, "strict")
    assert _types(engine, table) == narrow
    row = _row(policy, table)
    assert (int(row["n"]), float(row["f"])) == (5, 1.5)


def strict_fails_a_number_its_column_cannot_hold(
    policy: Policy, table: str, engine: Engine
) -> None:
    policy.drop(table)
    _create(engine, table)
    narrow = _types(engine, table)
    batch = pl.LazyFrame(OVERFLOWING, schema=BATCH_SCHEMA)
    with pytest.raises(Exception):
        policy.write(table, batch, APPEND, "strict")
    assert _types(engine, table) == narrow
    assert policy.read(table).height == 0


WIDENING_CASES = [
    pytest.param(update_widens_a_number_column, id="update-widens-numbers"),
    pytest.param(
        strict_leaves_a_fitting_number_to_the_database, id="strict-fitting-numbers"
    ),
    pytest.param(
        strict_fails_a_number_its_column_cannot_hold, id="strict-overflowing-number"
    ),
]
