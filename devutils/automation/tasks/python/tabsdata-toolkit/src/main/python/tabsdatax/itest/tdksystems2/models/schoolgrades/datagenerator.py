#
# Copyright 2026 Tabsdata Inc.
#

"""Data generator for the ``schoolgrades`` model.

A school academic dataset — academic years, departments, teachers, subjects,
classes, students, enrollments, assessments and grades — scaled by the requested
size (the student count = the size bucket); teachers / subjects / years scale
from it. Reference-only: there is no delta recipe, so ``create-data`` works but
``modify-data`` has no entities to change.
"""

from __future__ import annotations

import random
from collections import defaultdict
from datetime import date, datetime, timedelta

from tabsdatax.itest.tdksystems2.datagen import Entity, Model
from tabsdatax.itest.tdksystems2.emit import Column, Table

_BASE_SEED = 42

# Sizing (from the v1 sizing.json): the student count is the size bucket, and the
# other populations scale from it against this base.
_PROLIFIC_BASE = 500
_DEFAULT_TEACHERS = 40
_DEFAULT_SUBJECTS = 30
_DEFAULT_YEARS = 3
_CAP_SUBJECTS = 30

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

# (code_prefix, name, credits, dept_idx)
SUBJECT_POOL = [
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
    None,
    "Late submission; 10% penalty applied.",
    "Medical exemption; grade carried from previous attempt.",
    "Outstanding work.",
    "Borderline pass.",
    "Resit required.",
]

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


def _rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def _fmt_date(d) -> str:
    return d.strftime("%Y-%m-%d") if isinstance(d, (date, datetime)) else d


def _rand_name(sex: str = "M") -> str:
    first = random.choice(FIRST_M if sex == "M" else FIRST_F)
    return f"{first} {random.choice(LAST)}"


def _scale(default: int, n: int, base: int, cap: int | None = None) -> int:
    value = max(default * n // base, 1)
    return min(value, cap) if cap is not None else value


def gen_academic_years(n: int) -> list[dict]:
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


def gen_departments(pool=None) -> list[dict]:
    pool = pool or DEPARTMENTS
    return [{"dept_id": i, "name": name} for i, name in enumerate(pool, 1)]


def gen_teachers(n: int, departments: list[dict]) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        sex = random.choice(["M", "F"])
        name = _rand_name(sex)
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


def gen_subjects(pool=None, departments=None) -> list[dict]:
    pool = pool or SUBJECT_POOL
    rows = []
    if departments:
        dept_ids = [d["dept_id"] for d in departments]
    else:
        dept_ids = list(range(1, 9))
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


def gen_classes(
    subjects: list[dict],
    teachers: list[dict],
    academic_years: list[dict],
    sections_per_subject: int = 2,
) -> list[dict]:
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


def gen_students(n: int, academic_years: list[dict]) -> list[dict]:
    rows = []
    dob_start = date(2000, 1, 1)
    dob_end = date(2008, 12, 31)
    for i in range(1, n + 1):
        sex = random.choice(["M", "F"])
        name = _rand_name(sex)
        last = name.split()[-1].lower()
        dob = _rand_date(dob_start, dob_end)
        yr = random.choice(academic_years)
        rows.append({
            "student_id": i,
            "student_no": f"STU-{yr['label'][:4]}-{i:04d}",
            "name": name,
            "date_of_birth": _fmt_date(dob),
            "email": f"{last}{i}@school.edu",
            "enrolled_year_id": yr["year_id"],
        })
    return rows


def gen_enrollments(
    students: list[dict],
    classes: list[dict],
    academic_years: list[dict],
) -> list[dict]:
    """Each student enrolls in 4-7 classes within their year."""
    rows = []
    eid = 1
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


def gen_assessments(classes: list[dict], academic_years: list[dict]) -> list[dict]:
    rows = []
    aid = 1
    year_map = {y["year_id"]: y for y in academic_years}
    for cls in classes:
        yr = year_map[cls["year_id"]]
        yr_start = datetime.strptime(yr["start_date"], "%Y-%m-%d")
        yr_end = datetime.strptime(yr["end_date"], "%Y-%m-%d")
        n_ass = random.randint(2, 4)
        templates = random.sample(ASSESSMENT_TEMPLATES, n_ass)
        raw_weights = [t[2] for t in templates]
        total = sum(raw_weights)
        weights = [round(w / total * 100, 2) for w in raw_weights]
        weights[-1] = round(100.0 - sum(weights[:-1]), 2)
        for (name, atype, _), weight in zip(templates, weights):
            due = _rand_date(yr_start + timedelta(days=30), yr_end - timedelta(days=10))
            rows.append({
                "assessment_id": aid,
                "class_id": cls["class_id"],
                "name": name,
                "type": atype,
                "weight": weight,
                "max_score": 100.00,
                "due_date": _fmt_date(due),
            })
            aid += 1
    return rows


def gen_grades(enrollments: list[dict], assessments: list[dict]) -> list[dict]:
    """For each enrollment, generate grades for assessments in that class."""
    rows = []
    gid = 1
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
                if random.random() < 0.05:
                    continue
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


class _SchoolGrades(Model):
    entities: list[Entity] = []  # reference-only: no deltas

    def build_initial(self, rng: random.Random, n: int) -> list[Table]:
        random.seed(_BASE_SEED)
        n_students = n
        n_teachers = _scale(_DEFAULT_TEACHERS, n, _PROLIFIC_BASE)
        n_subjects = _scale(_DEFAULT_SUBJECTS, n, _PROLIFIC_BASE, cap=_CAP_SUBJECTS)
        n_years = _scale(_DEFAULT_YEARS, n, _PROLIFIC_BASE)
        n_subjects = min(n_subjects, len(SUBJECT_POOL))

        academic_years = gen_academic_years(n_years)
        departments = gen_departments()
        teachers = gen_teachers(n_teachers, departments)
        subjects = gen_subjects(SUBJECT_POOL[:n_subjects], departments)
        classes = gen_classes(subjects, teachers, academic_years)
        students = gen_students(n_students, academic_years)
        enrollments = gen_enrollments(students, classes, academic_years)
        assessments = gen_assessments(classes, academic_years)
        grades = gen_grades(enrollments, assessments)

        return [
            Table("academic_years", YEAR_COLS, academic_years),
            Table("departments", DEPT_COLS, departments),
            Table("teachers", TEACHER_COLS, teachers),
            Table("subjects", SUBJECT_COLS, subjects),
            Table("classes", CLASS_COLS, classes),
            Table("students", STUDENT_COLS, students),
            Table("enrollments", ENROLL_COLS, enrollments),
            Table("assessments", ASSESS_COLS, assessments),
            Table("grades", GRADE_COLS, grades),
        ]


def datagenerator() -> Model:
    return _SchoolGrades()
