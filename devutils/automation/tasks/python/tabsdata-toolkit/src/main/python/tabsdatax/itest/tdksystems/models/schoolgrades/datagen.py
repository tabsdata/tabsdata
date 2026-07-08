#!/usr/bin/env python3
#
# Copyright 2026 Tabsdata Inc.
#

"""
Data generator — School Classes and Student Grades System
Schema: sch_academic_years, sch_departments, sch_teachers, sch_subjects,
        sch_classes, sch_students, sch_enrollments, sch_assessments, sch_grades

Usage:
    python gen_06_school_grades.py [options]

Key table controls:
    --students    Number of students        (default: 500)
    --teachers    Number of teachers        (default: 40)
    --subjects    Number of subjects        (default: 30)
    --years       Number of academic years  (default: 3)
"""

import argparse
import random
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from tabsdatax.itest.tdksystems.emit import Column, Table, write_tables  # noqa: E402

BASE_SEED = 42

# ---------------------------------------------------------------------------
# Pools
# ---------------------------------------------------------------------------
FIRST_M = [
    "Liam",
    "Noah",
    "Ethan",
    "Mateo",
    "Carlos",
    "Jae",
    "Ivan",
    "Lucas",
    "Omar",
    "Stefan",
    "Raj",
    "Pablo",
    "Erik",
    "Marco",
    "Ali",
    "David",
    "James",
    "Tom",
    "Kwame",
    "George",
    "Andreu",
    "Roberto",
    "Tomás",
    "Javier",
    "Miguel",
    "Sérgio",
    "Iñigo",
    "Pierre",
    "Dmitri",
    "Yusuf",
]
FIRST_F = [
    "Sophia",
    "Ava",
    "Chloe",
    "Amara",
    "Elena",
    "Priya",
    "Nadia",
    "Dana",
    "Emma",
    "Natalie",
    "Beatriz",
    "Marta",
    "Giulia",
    "Claire",
    "Rachel",
    "Fatima",
    "Ana",
    "Yuki",
    "Sara",
    "Linh",
    "Helen",
    "Julia",
    "Chioma",
    "Isabelle",
    "Raquel",
    "Catarina",
    "Sofía",
    "Laura",
    "Isabel",
    "Claudia",
]
LAST = [
    "Chen",
    "Patel",
    "Rossi",
    "Kim",
    "Müller",
    "Dubois",
    "Diallo",
    "García",
    "Soler",
    "Puig",
    "Lemos",
    "Nunes",
    "Martín",
    "López",
    "Sánchez",
    "Fernández",
    "González",
    "Rodríguez",
    "Pérez",
    "Gómez",
    "Ruiz",
    "Díaz",
    "Alves",
    "Ferreira",
    "Costa",
    "Santos",
    "Oliveira",
    "Turner",
    "Strauss",
    "Petrov",
    "Novak",
    "Asante",
    "Okafor",
    "Brandt",
    "Mehta",
    "Nguyen",
]

DEPARTMENTS = [
    "Mathematics & Physics",
    "Computer Science",
    "Languages & Humanities",
    "Natural Sciences",
    "Arts & Physical Education",
    "Social Sciences",
    "Engineering",
    "Business & Economics",
]

SUBJECT_POOL = [
    # (code_prefix, name, credits, dept_idx)
    ("MATH", "Calculus I", 4, 0),
    ("MATH", "Calculus II", 4, 0),
    ("MATH", "Linear Algebra", 3, 0),
    ("MATH", "Statistics & Probability", 3, 0),
    ("PHYS", "Classical Mechanics", 4, 0),
    ("PHYS", "Electromagnetism", 4, 0),
    ("CS", "Intro to Programming", 4, 1),
    ("CS", "Data Structures & Algorithms", 4, 1),
    ("CS", "Operating Systems", 3, 1),
    ("CS", "Database Systems", 3, 1),
    ("CS", "Computer Networks", 3, 1),
    ("CS", "Machine Learning", 4, 1),
    ("ENG", "Academic English", 3, 2),
    ("ENG", "Technical Writing", 3, 2),
    ("HIST", "Modern History", 3, 2),
    ("PHIL", "Introduction to Philosophy", 3, 2),
    ("BIO", "Cell Biology", 3, 3),
    ("BIO", "Genetics", 3, 3),
    ("CHEM", "General Chemistry", 4, 3),
    ("ENV", "Environmental Science", 3, 3),
    ("ART", "Visual Arts Studio", 3, 4),
    ("PE", "Physical Education", 2, 4),
    ("SOC", "Introduction to Sociology", 3, 5),
    ("ECON", "Microeconomics", 3, 7),
    ("ECON", "Macroeconomics", 3, 7),
    ("BUS", "Principles of Management", 3, 7),
    ("ENG2", "Structures & Materials", 4, 6),
    ("ENG2", "Thermodynamics", 4, 6),
    ("MATH", "Discrete Mathematics", 3, 0),
    ("CS", "Software Engineering", 4, 1),
]

ASSESSMENT_TEMPLATES = [
    ("Midterm Exam", "exam", 35.00),
    ("Final Exam", "exam", 40.00),
    ("Assignment 1", "assignment", 10.00),
    ("Assignment 2", "assignment", 10.00),
    ("Lab Report", "lab", 15.00),
    ("Project", "project", 25.00),
    ("Oral Presentation", "oral", 15.00),
    ("Quiz 1", "test", 5.00),
    ("Quiz 2", "test", 5.00),
]

GRADE_NOTES = [
    None,
    None,
    None,  # most have no notes
    "Late submission; 10% penalty applied.",
    "Medical exemption; grade carried from previous attempt.",
    "Outstanding work.",
    "Borderline pass.",
    "Resit required.",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def esc(s):
    return s.replace("'", "''")


def sql_str(s):
    return f"'{esc(str(s))}'" if s is not None else "NULL"


def sql_int(v):
    return str(v) if v is not None else "NULL"


def rand_date(start, end):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def fmt_date(d):
    return d.strftime("%Y-%m-%d") if isinstance(d, (date, datetime)) else d


def rand_name(sex="M"):
    first = random.choice(FIRST_M if sex == "M" else FIRST_F)
    return f"{first} {random.choice(LAST)}"


def chunk_inserts(rows, table, columns, chunk=500):
    # One row per INSERT — portable across PostgreSQL, MySQL,
    # MSSQL, Oracle, and DB2 (Oracle doesn't accept multi-row VALUES).
    return [f"INSERT INTO {table} ({columns}) VALUES ({r});" for r in rows]


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------


def gen_academic_years(n):
    rows = []
    base = 2020
    for i in range(1, n + 1):
        y = base + (i - 1)
        rows.append({
            "year_id": i,
            "label": f"{y}-{str(y + 1)[2:]}",
            "start_date": f"{y}-09-10",
            "end_date": f"{y + 1}-06-20",
        })
    return rows


def gen_departments(pool=None):
    pool = pool or DEPARTMENTS
    rows = []
    for i, name in enumerate(pool, 1):
        rows.append({"dept_id": i, "name": name})
    return rows


def gen_teachers(n, departments):
    rows = []
    for i in range(1, n + 1):
        sex = random.choice(["M", "F"])
        name = rand_name(sex)
        last = name.split()[-1].lower()
        title = (
            random.choice(["Prof.", "Dr.", "Mr.", "Ms."])
            if random.random() < 0.6
            else ""
        )
        rows.append({
            "teacher_id": i,
            "employee_no": f"TCH-{i:03d}",
            "name": f"{title} {name}".strip(),
            "email": f"{last}{i}@school.edu",
            "dept_id": random.choice(departments)["dept_id"],
        })
    return rows


def gen_subjects(pool=None, departments=None):
    pool = pool or SUBJECT_POOL
    rows = []
    dept_ids = [d["dept_id"] for d in departments] if departments else list(range(1, 9))
    for i, (prefix, name, credits, dept_idx) in enumerate(pool, 1):
        dept_id = dept_ids[dept_idx] if dept_idx < len(dept_ids) else dept_ids[-1]
        rows.append({
            "subject_id": i,
            "code": f"{prefix}{i:03d}",
            "name": name,
            "credits": credits,
            "dept_id": dept_id,
        })
    return rows


def gen_classes(subjects, teachers, academic_years, sections_per_subject=2):
    rows = []
    cid = 1
    days_opts = ["Mon/Wed", "Tue/Thu", "Mon/Wed/Fri", "Tue/Thu/Fri"]
    time_opts = [
        "08:00-09:30",
        "09:00-10:30",
        "10:30-12:00",
        "11:00-12:30",
        "13:00-14:30",
        "14:00-15:30",
        "15:30-17:00",
        "16:00-17:30",
    ]
    rooms = (
        [f"Room {n}" for n in range(101, 120)]
        + [f"Lab {n}" for n in range(101, 108)]
        + [f"Lecture Hall {c}" for c in ["A", "B", "C"]]
    )
    for yr in academic_years:
        for subj in subjects:
            n_sec = random.randint(1, sections_per_subject)
            for s in range(n_sec):
                section = chr(65 + s)
                teacher = random.choice(teachers)
                rows.append({
                    "class_id": cid,
                    "subject_id": subj["subject_id"],
                    "teacher_id": teacher["teacher_id"],
                    "year_id": yr["year_id"],
                    "section": section,
                    "schedule": (
                        f"{random.choice(days_opts)} {random.choice(time_opts)}"
                    ),
                    "room": random.choice(rooms),
                    "max_students": random.choice([25, 28, 30, 32, 35, 40]),
                })
                cid += 1
    return rows


def gen_students(n, academic_years):
    rows = []
    dob_start = date(2000, 1, 1)
    dob_end = date(2008, 12, 31)
    for i in range(1, n + 1):
        sex = random.choice(["M", "F"])
        name = rand_name(sex)
        last = name.split()[-1].lower()
        dob = rand_date(dob_start, dob_end)
        yr = random.choice(academic_years)
        rows.append({
            "student_id": i,
            "student_no": f"STU-{yr['label'][:4]}-{i:04d}",
            "name": name,
            "date_of_birth": fmt_date(dob),
            "email": f"{last}{i}@school.edu",
            "enrolled_year_id": yr["year_id"],
        })
    return rows


def gen_enrollments(students, classes, academic_years):
    """Each student enrolls in 4-7 classes within their year."""
    rows = []
    eid = 1
    # Build year -> classes lookup
    from collections import defaultdict

    classes_by_year = defaultdict(list)
    for cls in classes:
        classes_by_year[cls["year_id"]].append(cls)

    for stu in students:
        yr_id = stu["enrolled_year_id"]
        yr_classes = classes_by_year.get(yr_id, [])
        if not yr_classes:
            continue
        n_enroll = random.randint(4, min(7, len(yr_classes)))
        sampled = random.sample(yr_classes, n_enroll)
        yr_obj = next(y for y in academic_years if y["year_id"] == yr_id)
        enroll_date = yr_obj["start_date"]
        for cls in sampled:
            rows.append({
                "enrollment_id": eid,
                "student_id": stu["student_id"],
                "class_id": cls["class_id"],
                "enrolled_on": enroll_date,
            })
            eid += 1
    return rows


def gen_assessments(classes, academic_years):
    rows = []
    aid = 1
    year_map = {y["year_id"]: y for y in academic_years}
    for cls in classes:
        yr = year_map[cls["year_id"]]
        yr_start = datetime.strptime(yr["start_date"], "%Y-%m-%d")
        yr_end = datetime.strptime(yr["end_date"], "%Y-%m-%d")
        # Pick 2-4 assessments with weights summing to ~100
        n_ass = random.randint(2, 4)
        templates = random.sample(ASSESSMENT_TEMPLATES, n_ass)
        # Normalize weights
        raw_weights = [t[2] for t in templates]
        total = sum(raw_weights)
        weights = [round(w / total * 100, 2) for w in raw_weights]
        weights[-1] = round(100.0 - sum(weights[:-1]), 2)  # fix rounding
        for (name, atype, _), weight in zip(templates, weights):
            due = rand_date(yr_start + timedelta(days=30), yr_end - timedelta(days=10))
            rows.append({
                "assessment_id": aid,
                "class_id": cls["class_id"],
                "name": name,
                "type": atype,
                "weight": weight,
                "max_score": 100.00,
                "due_date": fmt_date(due),
            })
            aid += 1
    return rows


def gen_grades(enrollments, assessments):
    """For each enrollment, generate grades for assessments in that class."""
    rows = []
    gid = 1
    from collections import defaultdict

    assess_by_class = defaultdict(list)
    for a in assessments:
        assess_by_class[a["class_id"]].append(a)

    enroll_map = defaultdict(list)
    for e in enrollments:
        enroll_map[e["class_id"]].append(e)

    for cls_id, cls_enrollments in enroll_map.items():
        cls_assessments = assess_by_class.get(cls_id, [])
        for enr in cls_enrollments:
            for ass in cls_assessments:
                # 5% chance of missing grade (absent/incomplete)
                if random.random() < 0.05:
                    continue
                # Model realistic grade distribution: slightly right-skewed
                mu = random.uniform(60, 85)
                sigma = random.uniform(8, 18)
                score = max(0, min(100, random.gauss(mu, sigma)))
                rows.append({
                    "grade_id": gid,
                    "enrollment_id": enr["enrollment_id"],
                    "assessment_id": ass["assessment_id"],
                    "score": round(score, 2),
                    "notes": random.choice(GRADE_NOTES),
                })
                gid += 1
    return rows


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

YEAR_COLS = [
    Column("year_id", "int", nullable=False, auto=True),
    Column("label", "varchar(20)", nullable=False),
    Column("start_date", "date", nullable=False),
    Column("end_date", "date", nullable=False),
]
DEPT_COLS = [
    Column("dept_id", "int", nullable=False, auto=True),
    Column("name", "varchar(100)", nullable=False),
]
TEACHER_COLS = [
    Column("teacher_id", "int", nullable=False, auto=True),
    Column("employee_no", "varchar(20)", nullable=False),
    Column("name", "varchar(100)", nullable=False),
    Column("email", "varchar(255)", nullable=False),
    Column("dept_id", "int"),
]
SUBJECT_COLS = [
    Column("subject_id", "int", nullable=False, auto=True),
    Column("code", "varchar(10)", nullable=False),
    Column("name", "varchar(150)", nullable=False),
    Column("credits", "smallint", nullable=False),
    Column("dept_id", "int"),
]
CLASS_COLS = [
    Column("class_id", "int", nullable=False, auto=True),
    Column("subject_id", "int", nullable=False),
    Column("teacher_id", "int", nullable=False),
    Column("year_id", "int", nullable=False),
    Column("section", "varchar(5)", nullable=False),
    Column("schedule", "varchar(100)"),
    Column("room", "varchar(20)"),
    Column("max_students", "smallint", nullable=False),
]
STUDENT_COLS = [
    Column("student_id", "int", nullable=False, auto=True),
    Column("student_no", "varchar(15)", nullable=False),
    Column("name", "varchar(100)", nullable=False),
    Column("date_of_birth", "date", nullable=False),
    Column("email", "varchar(255)"),
    Column("enrolled_year_id", "int", nullable=False),
]
ENROLL_COLS = [
    Column("enrollment_id", "int", nullable=False, auto=True),
    Column("student_id", "int", nullable=False),
    Column("class_id", "int", nullable=False),
    Column("enrolled_on", "date", nullable=False),
]
ASSESS_COLS = [
    Column("assessment_id", "int", nullable=False, auto=True),
    Column("class_id", "int", nullable=False),
    Column("name", "varchar(100)", nullable=False),
    Column("type", "varchar(20)", nullable=False),
    Column("weight", "decimal(5,2)", nullable=False),
    Column("max_score", "decimal(6,2)", nullable=False),
    Column("due_date", "date"),
]
GRADE_COLS = [
    Column("grade_id", "int", nullable=False, auto=True),
    Column("enrollment_id", "int", nullable=False),
    Column("assessment_id", "int", nullable=False),
    Column("score", "decimal(6,2)", nullable=False),
    Column("notes", "varchar(300)"),
]


def _legacy_render_unused(
    academic_years,
    departments,
    teachers,
    subjects,
    classes,
    students,
    enrollments,
    assessments,
    grades,
):
    lines = [
        "-- =============================================================",
        "-- Generated data — School Classes and Student Grades System",
        f"-- Students: {len(students)}  Teachers: {len(teachers)}  "
        f"Subjects: {len(subjects)}",
        f"-- Classes: {len(classes)}  Enrollments: {len(enrollments)}",
        f"-- Assessments: {len(assessments)}  Grades: {len(grades)}",
        "-- Compatible with MySQL and PostgreSQL",
        "-- =============================================================",
        "",
        "-- Academic years",
    ]
    yr_rows = [
        f"{sql_str(r['label'])},{sql_str(r['start_date'])},{sql_str(r['end_date'])}"
        for r in academic_years
    ]
    lines += chunk_inserts(yr_rows, "sch_academic_years", "label,start_date,end_date")

    lines += ["", "-- Departments"]
    dept_rows = [f"{sql_str(r['name'])}" for r in departments]
    lines += chunk_inserts(dept_rows, "sch_departments", "name")

    lines += ["", "-- Teachers"]
    tch_rows = [
        f"{sql_str(r['employee_no'])},{sql_str(r['name'])},{sql_str(r['email'])},{r['dept_id']}"
        for r in teachers
    ]
    lines += chunk_inserts(tch_rows, "sch_teachers", "employee_no,name,email,dept_id")

    lines += ["", "-- Subjects"]
    subj_rows = [
        f"{sql_str(r['code'])},{sql_str(r['name'])},{r['credits']},{r['dept_id']}"
        for r in subjects
    ]
    lines += chunk_inserts(subj_rows, "sch_subjects", "code,name,credits,dept_id")

    lines += ["", "-- Classes"]
    cls_rows = [
        f"{r['subject_id']},{r['teacher_id']},{r['year_id']},{sql_str(r['section'])},"
        f"{sql_str(r['schedule'])},{sql_str(r['room'])},{r['max_students']}"
        for r in classes
    ]
    lines += chunk_inserts(
        cls_rows,
        "sch_classes",
        "subject_id,teacher_id,year_id,section,schedule,room,max_students",
    )

    lines += ["", "-- Students"]
    stu_rows = [
        f"{sql_str(r['student_no'])},{sql_str(r['name'])},{sql_str(r['date_of_birth'])},"
        f"{sql_str(r['email'])},{r['enrolled_year_id']}"
        for r in students
    ]
    lines += chunk_inserts(
        stu_rows, "sch_students", "student_no,name,date_of_birth,email,enrolled_year_id"
    )

    lines += ["", "-- Enrollments"]
    enr_rows = [
        f"{r['student_id']},{r['class_id']},{sql_str(r['enrolled_on'])}"
        for r in enrollments
    ]
    lines += chunk_inserts(
        enr_rows, "sch_enrollments", "student_id,class_id,enrolled_on"
    )

    lines += ["", "-- Assessments"]
    ass_rows = [
        f"{r['class_id']},{sql_str(r['name'])},{sql_str(r['type'])},{r['weight']},"
        f"{r['max_score']},{sql_str(r['due_date'])}"
        for r in assessments
    ]
    lines += chunk_inserts(
        ass_rows, "sch_assessments", "class_id,name,type,weight,max_score,due_date"
    )

    lines += ["", "-- Grades"]
    grd_rows = [
        f"{r['enrollment_id']},{r['assessment_id']},{r['score']},{sql_str(r['notes'])}"
        for r in grades
    ]
    lines += chunk_inserts(
        grd_rows, "sch_grades", "enrollment_id,assessment_id,score,notes"
    )

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_tables(args):
    random.seed(args.seed)
    n_subjects = min(args.subjects, len(SUBJECT_POOL))
    print("Generating school grades data...", file=sys.stderr)
    academic_years = gen_academic_years(args.years)
    departments = gen_departments()
    teachers = gen_teachers(args.teachers, departments)
    subjects = gen_subjects(SUBJECT_POOL[:n_subjects], departments)
    classes = gen_classes(subjects, teachers, academic_years)
    students = gen_students(args.students, academic_years)
    enrollments = gen_enrollments(students, classes, academic_years)
    assessments = gen_assessments(classes, academic_years)
    grades = gen_grades(enrollments, assessments)
    print(
        f"  years={len(academic_years)} depts={len(departments)} "
        f"teachers={len(teachers)} subjects={len(subjects)} classes={len(classes)} "
        f"students={len(students)} enrollments={len(enrollments)} "
        f"assessments={len(assessments)} grades={len(grades)}",
        file=sys.stderr,
    )

    return [
        Table("sch_academic_years", YEAR_COLS, academic_years),
        Table("sch_departments", DEPT_COLS, departments),
        Table("sch_teachers", TEACHER_COLS, teachers),
        Table("sch_subjects", SUBJECT_COLS, subjects),
        Table("sch_classes", CLASS_COLS, classes),
        Table("sch_students", STUDENT_COLS, students),
        Table("sch_enrollments", ENROLL_COLS, enrollments),
        Table("sch_assessments", ASSESS_COLS, assessments),
        Table("sch_grades", GRADE_COLS, grades),
    ]


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--students", type=int, default=500)
    p.add_argument("--teachers", type=int, default=40)
    p.add_argument("--subjects", type=int, default=30)
    p.add_argument("--years", type=int, default=3)
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
