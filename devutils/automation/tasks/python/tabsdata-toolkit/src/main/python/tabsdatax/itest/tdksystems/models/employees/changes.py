#
# Copyright 2026 Tabsdata Inc.
#

"""Change recipe for the `employees` model — see
`tabsdatax.itest.tdksystems.changes` for the contract.

Two mutable entities: employees (insert / update / delete, keyed by the
explicit `emp_no` which the generator assigns from 10001) and departments
(update only — its 4-char `dept_no` is too small a key space to mint endless
unique inserts, and other tables reference it so it can't be deleted from). New
employees get an `emp_no` in a high, non-colliding range (`9_000_000 +
ordinal`), so inserts never clash with the seed or with each other."""

from __future__ import annotations

import random
from datetime import date

from tabsdatax.itest.tdksystems.changes import ChangeModel, Entity, OpKind
from tabsdatax.itest.tdksystems.emit import Column

# The generator assigns emp_no from this base; inserts start well above the
# largest seed (xl = 1,000,000 employees) so they never collide.
_EMP_SEED_BASE = 10001
_EMP_INSERT_BASE = 9_000_000


class _Employees(Entity):
    table = "emp_employees"
    columns = [
        Column("emp_no", "int", nullable=False),
        Column("birth_date", "date", nullable=False),
        Column("first_name", "varchar(14)", nullable=False),
        Column("last_name", "varchar(16)", nullable=False),
        Column("gender", "char(1)", nullable=False),
        Column("hire_date", "date", nullable=False),
    ]
    supports = (OpKind.INSERT, OpKind.UPDATE, OpKind.DELETE)

    def new_row(self, rng: random.Random, i: int) -> dict:
        return {
            "emp_no": _EMP_INSERT_BASE + i,
            "birth_date": date(1980, 1, 1),
            "first_name": f"New{i}"[:14],
            "last_name": f"Hire{i}"[:16],
            "gender": "M" if i % 2 == 0 else "F",
            "hire_date": date(2020, 1, 1),
        }

    def target(self, i: int, existing: int) -> dict:
        return {"emp_no": _EMP_SEED_BASE + i}

    def mutate(self, rng: random.Random, i: int, existing: int) -> dict:
        return {"first_name": f"Upd{i}"[:14], "last_name": f"Chg{i}"[:16]}


class _Departments(Entity):
    table = "emp_departments"
    columns = [
        Column("dept_no", "char(4)", nullable=False),
        Column("dept_name", "varchar(40)", nullable=False),
    ]
    # Update only: dept_no (char(4)) is too small for endless unique inserts and
    # other tables reference it, so it is never inserted into or deleted from.
    supports = (OpKind.UPDATE,)

    def target(self, i: int, existing: int) -> dict:
        # The 9 seeded departments are d001..d009.
        return {"dept_no": f"d{(i % 9) + 1:03d}"}

    def mutate(self, rng: random.Random, i: int, existing: int) -> dict:
        return {"dept_name": f"Renamed Dept {i}"[:40]}


def recipe() -> ChangeModel:
    return ChangeModel(entities=[_Employees(), _Departments()])
