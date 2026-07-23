#
# Copyright 2026 Tabsdata Inc.
#

"""Data generator for the ``employees`` model.

Builds a deterministic HR dataset — departments, employees, department
assignments, titles and salary history — scaled by the requested size (the
employee count = the size bucket). Seeded employees get ``emp_no`` from 10001,
so ``modify-data`` can target the i-th seeded row as ``emp_no = 10001 + i``.
Two entities take deltas: employees (insert / update / delete; inserts mint an
``emp_no`` in a high, non-colliding range) and departments (update only — its
4-char key is too small for endless unique inserts and other tables reference
it).
"""

from __future__ import annotations

import random
from datetime import date, timedelta

from tabsdatax.itest.tdksystems2.datagen import Entity, Model, OpKind
from tabsdatax.itest.tdksystems2.emit import Column, Table

_ACTIVE_TO = date(9999, 1, 1)
_EMP_SEED_BASE = 10001
_EMP_INSERT_BASE = 9_000_000

_DEPARTMENTS = [
    ("d001", "Marketing"),
    ("d002", "Finance"),
    ("d003", "Human Resources"),
    ("d004", "Production"),
    ("d005", "Development"),
    ("d006", "Quality Management"),
    ("d007", "Sales"),
    ("d008", "Research"),
    ("d009", "Customer Service"),
]

_TITLES = [
    ("Staff", 45000, 28),
    ("Senior Staff", 58000, 22),
    ("Engineer", 55000, 14),
    ("Senior Engineer", 72000, 18),
    ("Assistant Engineer", 42000, 8),
    ("Technique Leader", 80000, 8),
    ("Manager", 120000, 2),
]

_FIRST_NAMES = [
    "Georgi",
    "Bezalel",
    "Parto",
    "Chirstian",
    "Kyoichi",
    "Anneke",
    "Tzvetan",
    "Saniya",
    "Sumant",
    "Duangkaew",
    "Mary",
    "Patricio",
    "Eberhardt",
    "Kazuhito",
    "Cristinel",
    "Kazuhide",
    "Lillian",
    "Mayuko",
    "Ramzi",
    "Shahaf",
    "Bojan",
    "Hilari",
    "Yongqiao",
    "Berni",
    "Reuven",
    "Mokhtar",
    "Randi",
    "Hugo",
    "Ranjit",
    "Fanny",
    "Tonny",
    "Domenick",
    "Otmar",
    "Elvis",
    "Karsten",
    "Anna",
    "Joseph",
    "Nathan",
    "Patricia",
    "Satoshi",
]

_LAST_NAMES = [
    "Facello",
    "Simmel",
    "Bamford",
    "Koblick",
    "Maliniak",
    "Preusig",
    "Zielinski",
    "Kalloufi",
    "Peac",
    "Piveteau",
    "Sluis",
    "Bridgland",
    "Terkki",
    "Genin",
    "Bouloucos",
    "Cappelletti",
    "Eldridge",
    "Warwick",
    "Erde",
    "Famili",
    "Montemayor",
    "Pinochet",
    "Awdeh",
    "Falkenberg",
    "Kambil",
    "Adachi",
    "Yoshikawa",
    "Cools",
    "Veldwijk",
    "Reinhardt",
    "Kilian",
    "Birge",
    "Baker",
    "Baldwin",
    "Banerjee",
    "Barros",
    "Basu",
    "Xie",
    "Honda",
    "Alves",
]

_DEPT_COLS = [
    Column("dept_no", "char(4)", nullable=False),
    Column("dept_name", "varchar(40)", nullable=False),
]
_EMP_COLS = [
    Column("emp_no", "int", nullable=False),
    Column("birth_date", "date", nullable=False),
    Column("first_name", "varchar(14)", nullable=False),
    Column("last_name", "varchar(16)", nullable=False),
    Column("gender", "char(1)", nullable=False),
    Column("hire_date", "date", nullable=False),
]
_DEPT_EMP_COLS = [
    Column("emp_no", "int", nullable=False),
    Column("dept_no", "char(4)", nullable=False),
    Column("from_date", "date", nullable=False),
    Column("to_date", "date", nullable=False),
]
_TITLES_COLS = [
    Column("emp_no", "int", nullable=False),
    Column("title", "varchar(50)", nullable=False),
    Column("from_date", "date", nullable=False),
    Column("to_date", "date"),
]
_SALARIES_COLS = [
    Column("emp_no", "int", nullable=False),
    Column("salary", "int", nullable=False),
    Column("from_date", "date", nullable=False),
    Column("to_date", "date", nullable=False),
]


def _add_year(d: date) -> date:
    try:
        return d.replace(year=d.year + 1)
    except ValueError:
        return d.replace(year=d.year + 1, day=28)


def _gen_employees(rng: random.Random, n: int) -> list[dict]:
    rows = []
    hire_start = date(1985, 1, 1)
    span = (date(2002, 12, 31) - hire_start).days
    for i in range(n):
        hire = hire_start + timedelta(days=rng.randint(0, span))
        age = rng.randint(22, 58)
        birth = hire - timedelta(days=age * 365 + rng.randint(0, 365))
        rows.append({
            "emp_no": _EMP_SEED_BASE + i,
            "birth_date": birth,
            "first_name": rng.choice(_FIRST_NAMES)[:14],
            "last_name": rng.choice(_LAST_NAMES)[:16],
            "gender": rng.choice(["M", "F"]),
            "hire_date": hire,
        })
    return rows


def _gen_dept_emp(rng: random.Random, employees: list[dict]) -> list[dict]:
    rows = []
    for e in employees:
        d1 = rng.choice(_DEPARTMENTS)
        if rng.random() >= 0.25:
            rows.append({
                "emp_no": e["emp_no"],
                "dept_no": d1[0],
                "from_date": e["hire_date"],
                "to_date": _ACTIVE_TO,
            })
        else:
            tenure_days = (date(2020, 1, 1) - e["hire_date"]).days
            split = e["hire_date"] + timedelta(
                days=rng.randint(365, max(366, tenure_days - 30))
            )
            rows.append({
                "emp_no": e["emp_no"],
                "dept_no": d1[0],
                "from_date": e["hire_date"],
                "to_date": split,
            })
            d2 = rng.choice([d for d in _DEPARTMENTS if d[0] != d1[0]])
            rows.append({
                "emp_no": e["emp_no"],
                "dept_no": d2[0],
                "from_date": split,
                "to_date": _ACTIVE_TO,
            })
    return rows


def _gen_dept_managers(rng: random.Random, employees: list[dict]) -> list[dict]:
    rows = []
    pool = rng.sample(employees, min(len(employees), len(_DEPARTMENTS) * 3))
    for d in _DEPARTMENTS:
        n_mgr = rng.randint(1, 3)
        cur_start = max(date(1985, 1, 1), min(e["hire_date"] for e in employees))
        for i in range(n_mgr):
            if not pool:
                break
            mgr = pool.pop()
            to_d = cur_start + timedelta(days=rng.randint(2, 8) * 365)
            if i == n_mgr - 1:
                to_d = _ACTIVE_TO
            rows.append({
                "emp_no": mgr["emp_no"],
                "dept_no": d[0],
                "from_date": cur_start,
                "to_date": to_d,
            })
            cur_start = to_d if to_d.year < 9999 else cur_start
    return rows


def _pick_title(rng: random.Random) -> tuple[str, int]:
    titles = [t[0] for t in _TITLES]
    weights = [t[2] for t in _TITLES]
    bases = {t[0]: t[1] for t in _TITLES}
    chosen = rng.choices(titles, weights=weights)[0]
    return chosen, bases[chosen]


def _gen_titles(rng: random.Random, employees: list[dict]) -> list[dict]:
    rows = []
    for e in employees:
        title, _ = _pick_title(rng)
        from_d = e["hire_date"]
        if rng.random() < 0.35:
            mid = from_d + timedelta(days=rng.randint(2 * 365, 6 * 365))
            rows.append({
                "emp_no": e["emp_no"],
                "title": title,
                "from_date": from_d,
                "to_date": mid,
            })
            rows.append({
                "emp_no": e["emp_no"],
                "title": rng.choice([t[0] for t in _TITLES]),
                "from_date": mid,
                "to_date": None,
            })
        else:
            rows.append({
                "emp_no": e["emp_no"],
                "title": title,
                "from_date": from_d,
                "to_date": None,
            })
    return rows


def _gen_salaries(rng: random.Random, employees: list[dict]) -> list[dict]:
    rows = []
    today = date(2002, 12, 31)
    for e in employees:
        _, base = _pick_title(rng)
        salary = int(base * rng.uniform(0.85, 1.15))
        if rng.random() < 0.7:
            end = today
        else:
            tenure_days = max(365, (today - e["hire_date"]).days)
            end = e["hire_date"] + timedelta(days=rng.randint(365, tenure_days))
        cur = e["hire_date"]
        while cur < end:
            nxt = min(_add_year(cur), end)
            rows.append({
                "emp_no": e["emp_no"],
                "salary": salary,
                "from_date": cur,
                "to_date": nxt,
            })
            salary = int(salary * rng.uniform(1.005, 1.035))
            cur = nxt
        if end == today and rng.random() < 0.7:
            rows.append({
                "emp_no": e["emp_no"],
                "salary": salary,
                "from_date": end,
                "to_date": _ACTIVE_TO,
            })
    return rows


class _Employees(Entity):
    table = "employees"
    columns = _EMP_COLS
    supports = (OpKind.INSERT, OpKind.UPDATE, OpKind.DELETE)
    rows_per_set = 1

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
    table = "departments"
    columns = _DEPT_COLS
    supports = (OpKind.UPDATE,)
    rows_per_set = 1

    def target(self, i: int, existing: int) -> dict:
        return {"dept_no": f"d{(i % 9) + 1:03d}"}

    def mutate(self, rng: random.Random, i: int, existing: int) -> dict:
        return {"dept_name": f"Renamed Dept {i}"[:40]}


class _EmployeesModel(Model):
    entities = [_Employees(), _Departments()]

    def build_initial(self, rng: random.Random, n: int) -> list[Table]:
        employees = _gen_employees(rng, n)
        dept_rows = [{"dept_no": d[0], "dept_name": d[1]} for d in _DEPARTMENTS]
        return [
            Table("departments", _DEPT_COLS, dept_rows),
            Table("employees", _EMP_COLS, employees),
            Table("dept_emp", _DEPT_EMP_COLS, _gen_dept_emp(rng, employees)),
            Table("dept_manager", _DEPT_EMP_COLS, _gen_dept_managers(rng, employees)),
            Table("titles", _TITLES_COLS, _gen_titles(rng, employees)),
            Table("salaries", _SALARIES_COLS, _gen_salaries(rng, employees)),
        ]


def datagenerator() -> Model:
    return _EmployeesModel()
