#!/usr/bin/env python3
#
# Copyright 2026 Tabsdata Inc.
#

"""
Data generator — Employees (HR system).

Generates department, employee, assignment, title and salary-history
data modelled on the canonical MySQL "employees" sample DB.

Tables:
    emp_departments emp_employees emp_dept_emp
    emp_dept_manager emp_titles emp_salaries

Usage:
    python datagen.py [--employees N] [--format FMT] [--out DIR]
"""

import argparse
import random
import sys
from datetime import date, timedelta
from pathlib import Path

from tabsdatax.itest.tdksystems.emit import Column, Table, write_tables  # noqa: E402

BASE_SEED = 42
ACTIVE_TO = date(9999, 1, 1)

DEPARTMENTS = [
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

TITLES = [
    ("Staff", 45000, 28),
    ("Senior Staff", 58000, 22),
    ("Engineer", 55000, 14),
    ("Senior Engineer", 72000, 18),
    ("Assistant Engineer", 42000, 8),
    ("Technique Leader", 80000, 8),
    ("Manager", 120000, 2),
]

FIRST_NAMES = [
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
    "Zvonko",
    "Marjo",
    "Suzette",
    "Prasadram",
    "Aamer",
    "Akira",
    "Anders",
    "Anna",
    "Bedir",
    "Carmine",
    "Constantijn",
    "Damminda",
    "Dekang",
    "Dines",
    "Eelco",
    "Elin",
    "Engin",
    "Eran",
    "Florian",
    "Gareth",
    "Gerardo",
    "Gino",
    "Hatem",
    "Heekyoung",
    "Helmar",
    "Hidefumi",
    "Hironobu",
    "Jaewon",
    "Janalyn",
    "Jasmine",
    "Joseph",
    "Karok",
    "Kazu",
    "Kenroku",
    "Khaled",
    "Kshitij",
    "Latish",
    "Magoichi",
    "Marcel",
    "Margareta",
    "Masaaki",
    "Matti",
    "Mihalis",
    "Mona",
    "Nathan",
    "Nidapan",
    "Niranjan",
    "Noriko",
    "Patricia",
    "Pradeep",
    "Premal",
    "Quan",
    "Rafols",
    "Saniya",
    "Satoshi",
    "Sergi",
    "Sreenivas",
    "Sudharsan",
    "Sumali",
    "Tamas",
    "Tobias",
    "Toney",
    "Toshiyuki",
    "Tuval",
    "Udi",
    "Vidya",
    "Volkan",
    "Weicheng",
    "Xiahua",
    "Yakov",
    "Yannick",
    "Yuhei",
    "Yuping",
    "Zdislav",
    "Zhenghong",
    "Zito",
    "Zsolt",
]

LAST_NAMES = [
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
    "Rajaonarivelo",
    "Veldwijk",
    "Reinhardt",
    "Kilian",
    "Birge",
    "Kohrt",
    "Arima",
    "Morini",
    "Krishnamurthy",
    "Laari",
    "Rissanen",
    "Sansone",
    "Schaap",
    "Wadsworth",
    "Kawazu",
    "Fortenberry",
    "Garigliano",
    "Hamano",
    "Honda",
    "Iwayanagi",
    "Joulia",
    "Kumagai",
    "Lounis",
    "Matsumura",
    "Mehrotra",
    "Onuegbe",
    "Pugachev",
    "Rosenberg",
    "Saraiva",
    "Surdeanu",
    "Tabashneck",
    "Trentini",
    "Verkamp",
    "Weedman",
    "Xie",
    "Yamafuji",
    "Zambonelli",
    "Aamodt",
    "Acharya",
    "Aingworth",
    "Ait-Sahalia",
    "Akiyama",
    "Allada",
    "Alves",
    "Aoyagi",
    "Appelt",
    "Asmuth",
    "Avouris",
    "Babu",
    "Bach",
    "Baek",
    "Bahceci",
    "Bahl",
    "Bain",
    "Baird",
    "Bairner",
    "Baker",
    "Bal",
    "Baldwin",
    "Bamberger",
    "Banerjee",
    "Bannister",
    "Baran",
    "Baranowski",
    "Barros",
    "Bartelt",
    "Basak",
    "Basu",
    "Bathory",
    "Battaiola",
]


def add_year(d):
    try:
        return d.replace(year=d.year + 1)
    except ValueError:
        return d.replace(year=d.year + 1, day=28)


def gen_employees(n, start=10001):
    rows = []
    hire_start = date(1985, 1, 1)
    span = (date(2002, 12, 31) - hire_start).days
    for i in range(n):
        hire = hire_start + timedelta(days=random.randint(0, span))
        age = random.randint(22, 58)
        birth = hire - timedelta(days=age * 365 + random.randint(0, 365))
        rows.append({
            "emp_no": start + i,
            "birth_date": birth,
            "first_name": random.choice(FIRST_NAMES)[:14],
            "last_name": random.choice(LAST_NAMES)[:16],
            "gender": random.choice(["M", "F"]),
            "hire_date": hire,
        })
    return rows


def gen_dept_emp(employees, depts):
    rows = []
    for e in employees:
        d1 = random.choice(depts)
        switched = random.random() < 0.25
        if not switched:
            rows.append({
                "emp_no": e["emp_no"],
                "dept_no": d1[0],
                "from_date": e["hire_date"],
                "to_date": ACTIVE_TO,
            })
        else:
            tenure_days = (date(2020, 1, 1) - e["hire_date"]).days
            split = e["hire_date"] + timedelta(
                days=random.randint(365, max(366, tenure_days - 30))
            )
            rows.append({
                "emp_no": e["emp_no"],
                "dept_no": d1[0],
                "from_date": e["hire_date"],
                "to_date": split,
            })
            d2 = random.choice([d for d in depts if d[0] != d1[0]])
            rows.append({
                "emp_no": e["emp_no"],
                "dept_no": d2[0],
                "from_date": split,
                "to_date": ACTIVE_TO,
            })
    return rows


def gen_dept_managers(employees, depts):
    rows = []
    pool = random.sample(employees, min(len(employees), len(depts) * 3))
    for d in depts:
        n_mgr = random.randint(1, 3)
        cur_start = max(date(1985, 1, 1), min(e["hire_date"] for e in employees))
        for i in range(n_mgr):
            if not pool:
                break
            mgr = pool.pop()
            term = random.randint(2, 8) * 365
            from_d = cur_start
            to_d = from_d + timedelta(days=term)
            if i == n_mgr - 1:
                to_d = ACTIVE_TO
            cur_start = to_d if to_d.year < 9999 else cur_start
            rows.append({
                "emp_no": mgr["emp_no"],
                "dept_no": d[0],
                "from_date": from_d,
                "to_date": to_d,
            })
    return rows


def _pick_title():
    titles = [t[0] for t in TITLES]
    weights = [t[2] for t in TITLES]
    bases = {t[0]: t[1] for t in TITLES}
    chosen = random.choices(titles, weights=weights)[0]
    return chosen, bases[chosen]


def gen_titles(employees):
    rows = []
    for e in employees:
        title, _ = _pick_title()
        from_d = e["hire_date"]
        if random.random() < 0.35:
            mid = from_d + timedelta(days=random.randint(2 * 365, 6 * 365))
            rows.append({
                "emp_no": e["emp_no"],
                "title": title,
                "from_date": from_d,
                "to_date": mid,
            })
            rows.append({
                "emp_no": e["emp_no"],
                "title": random.choice([t[0] for t in TITLES]),
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


def gen_salaries(employees):
    rows = []
    today = date(2002, 12, 31)
    for e in employees:
        _, base = _pick_title()
        salary = int(base * random.uniform(0.85, 1.15))
        if random.random() < 0.7:
            end = today
        else:
            tenure_days = max(365, (today - e["hire_date"]).days)
            end = e["hire_date"] + timedelta(days=random.randint(365, tenure_days))
        cur = e["hire_date"]
        while cur < end:
            nxt = min(add_year(cur), end)
            rows.append({
                "emp_no": e["emp_no"],
                "salary": salary,
                "from_date": cur,
                "to_date": nxt,
            })
            salary = int(salary * random.uniform(1.005, 1.035))
            cur = nxt
        if end == today and random.random() < 0.7:
            rows.append({
                "emp_no": e["emp_no"],
                "salary": salary,
                "from_date": end,
                "to_date": ACTIVE_TO,
            })
    return rows


# Column specs ---------------------------------------------------------------

DEPT_COLS = [
    Column("dept_no", "char(4)", nullable=False),
    Column("dept_name", "varchar(40)", nullable=False),
]
EMP_COLS = [
    Column("emp_no", "int", nullable=False),
    Column("birth_date", "date", nullable=False),
    Column("first_name", "varchar(14)", nullable=False),
    Column("last_name", "varchar(16)", nullable=False),
    Column("gender", "char(1)", nullable=False),
    Column("hire_date", "date", nullable=False),
]
DEPT_EMP_COLS = [
    Column("emp_no", "int", nullable=False),
    Column("dept_no", "char(4)", nullable=False),
    Column("from_date", "date", nullable=False),
    Column("to_date", "date", nullable=False),
]
DEPT_MGR_COLS = DEPT_EMP_COLS
TITLES_COLS = [
    Column("emp_no", "int", nullable=False),
    Column("title", "varchar(50)", nullable=False),
    Column("from_date", "date", nullable=False),
    Column("to_date", "date"),
]
SALARIES_COLS = [
    Column("emp_no", "int", nullable=False),
    Column("salary", "int", nullable=False),
    Column("from_date", "date", nullable=False),
    Column("to_date", "date", nullable=False),
]


def build_tables(args):
    random.seed(args.seed)
    print("Generating employees data...", file=sys.stderr)
    depts = DEPARTMENTS
    employees = gen_employees(args.employees)
    dept_emp = gen_dept_emp(employees, depts)
    dept_mgr = gen_dept_managers(employees, depts)
    titles = gen_titles(employees)
    salaries = gen_salaries(employees)
    print(
        f"  employees={len(employees)} dept_emp={len(dept_emp)} "
        f"dept_mgr={len(dept_mgr)} titles={len(titles)} salaries={len(salaries)}",
        file=sys.stderr,
    )

    dept_rows = [{"dept_no": d[0], "dept_name": d[1]} for d in depts]

    return [
        Table("emp_departments", DEPT_COLS, dept_rows),
        Table("emp_employees", EMP_COLS, employees),
        Table("emp_dept_emp", DEPT_EMP_COLS, dept_emp),
        Table("emp_dept_manager", DEPT_MGR_COLS, dept_mgr),
        Table("emp_titles", TITLES_COLS, titles),
        Table("emp_salaries", SALARIES_COLS, salaries),
    ]


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--employees", type=int, default=10000)
    p.add_argument("--format", choices=("sql", "csv", "json", "parquet"), default="sql")
    p.add_argument("--seed", type=int, default=BASE_SEED)
    p.add_argument("--out", required=True)
    p.add_argument("--chunks", type=int, default=None)
    return p.parse_args()


def main():
    args = parse_args()
    tables = build_tables(args)
    write_tables(
        tables,
        args.out,
        args.format,
        source_dir=Path(__file__).parent,
        chunks=args.chunks,
    )


if __name__ == "__main__":
    main()
