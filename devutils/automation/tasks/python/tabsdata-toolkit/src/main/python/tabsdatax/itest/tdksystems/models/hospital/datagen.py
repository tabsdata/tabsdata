#!/usr/bin/env python3
"""
Data generator — Hospital Patient System
Schema: hp_wards, hp_staff, hp_patients, hp_admissions,
        hp_appointments, hp_diagnoses, hp_prescriptions

Usage:
    python gen_02_hospital.py [options]

Key table controls:
    --patients       Number of patients          (default: 500)
    --staff          Number of staff members     (default: 80)
    --admissions     Number of admissions        (default: 600)
    --appointments   Number of appointments      (default: 1200)
"""

import argparse
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

from tabsdatax.itest.tdksystems.emit import Column, Table, write_tables  # noqa: E402

BASE_SEED = 42

# ---------------------------------------------------------------------------
# Name / data pools
# ---------------------------------------------------------------------------
FIRST_NAMES_M = [
    "George",
    "Ivan",
    "Kwame",
    "David",
    "James",
    "Carlos",
    "Raj",
    "Pablo",
    "Tom",
    "Marco",
    "Andreu",
    "Lucas",
    "Roberto",
    "Ethan",
    "Noah",
    "Liam",
    "Mateo",
    "Yusuf",
    "Pierre",
    "Erik",
    "Stefan",
    "Omar",
    "Ali",
    "Jae",
]
FIRST_NAMES_F = [
    "Helen",
    "Julia",
    "Fatima",
    "Rachel",
    "Sophia",
    "Claire",
    "Ana",
    "Linh",
    "Amara",
    "Ava",
    "Chloe",
    "Priya",
    "Sofia",
    "Yuki",
    "Nadia",
    "Elena",
    "Beatriz",
    "Marta",
    "Giulia",
    "Dana",
    "Emma",
    "Chioma",
    "Natalie",
    "Sara",
]
LAST_NAMES = [
    "Turner",
    "Strauss",
    "Petrov",
    "Novak",
    "Asante",
    "Al-Rashid",
    "Okafor",
    "Brandt",
    "Mehta",
    "Nguyen",
    "Ruiz",
    "Walsh",
    "Otieno",
    "Kovic",
    "García",
    "Soler",
    "Puig",
    "Lemos",
    "Nunes",
    "Chen",
    "Patel",
    "Rossi",
    "Kim",
    "Müller",
    "Dubois",
    "Diallo",
    "Ferrari",
    "Dupont",
    "Martin",
    "Laurent",
    "Voss",
    "Volkov",
    "Eze",
    "Santos",
    "Ivanova",
    "Peixoto",
    "Ferreira",
    "Bianchi",
    "Fitzgerald",
]

WARD_LIST = [
    ("Cardiology", 3, 24),
    ("General Medicine", 2, 40),
    ("Orthopaedics", 4, 20),
    ("Intensive Care Unit", 1, 10),
    ("Neurology", 3, 18),
    ("Paediatrics", 5, 16),
    ("Oncology", 4, 22),
    ("Respiratory", 2, 20),
    ("Gastroenterology", 3, 18),
    ("Haematology", 4, 14),
    ("Endocrinology", 2, 16),
    ("Rheumatology", 3, 12),
]

STAFF_ROLES_POOL = (
    ["doctor"] * 40
    + ["nurse"] * 40
    + ["surgeon"] * 10
    + ["radiologist"] * 5
    + ["pharmacist"] * 3
    + ["admin"] * 2
)

SPECIALIZATIONS = {
    "doctor": [
        "Cardiology",
        "General Medicine",
        "Neurology",
        "Paediatrics",
        "Oncology",
        "Respiratory",
        "Gastroenterology",
        "Haematology",
        "Endocrinology",
        "Rheumatology",
        "ICU",
        "Geriatrics",
    ],
    "surgeon": [
        "Orthopaedic Surgery",
        "Cardiac Surgery",
        "Neurosurgery",
        "General Surgery",
        "Vascular Surgery",
    ],
    "radiologist": ["Diagnostic Radiology", "Interventional Radiology"],
    "nurse": [None],
    "pharmacist": [None],
    "admin": [None],
}

UK_CITIES = ["London", "Manchester", "Birmingham", "Leeds", "Bristol", "Glasgow"]
BLOOD_TYPES = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
BT_WEIGHTS = [34, 6, 9, 2, 3, 1, 38, 7]  # approximate real-world %

ALLERGY_LIST = [
    "Penicillin",
    "Aspirin",
    "Sulfa drugs",
    "Latex",
    "Codeine",
    "NSAIDs",
    "Ibuprofen",
    "Morphine",
    "Contrast dye",
    "Erythromycin",
]

REASONS_FOR_ADMISSION = [
    "Chest pain and shortness of breath",
    "Severe asthma exacerbation",
    "Fractured right tibia",
    "TIA — transient ischaemic attack",
    "Acute appendicitis",
    "Haemorrhagic stroke; ICU admission",
    "Type 2 diabetes — poor glycaemic control",
    "Community-acquired pneumonia",
    "Acute kidney injury",
    "Hip replacement surgery",
    "Colorectal cancer staging",
    "Deep vein thrombosis",
    "Atrial fibrillation — rate control",
    "Sepsis — source under investigation",
    "Chronic obstructive pulmonary disease exacerbation",
    "Hypertensive urgency",
    "Acute leukaemia — induction chemotherapy",
    "Ruptured appendix",
    "Bowel obstruction",
    "Metastatic lung cancer — palliative care",
]

ICD10_POOL = [
    ("I50.0", "Congestive heart failure"),
    ("I10", "Essential hypertension"),
    ("J45.1", "Non-allergic asthma, acute exacerbation"),
    ("S82.2", "Fracture of shaft of tibia"),
    ("G45.9", "Transient cerebral ischaemic attack"),
    ("K37", "Unspecified appendicitis"),
    ("I61.9", "Intracerebral haemorrhage, unspecified"),
    ("E11.9", "Type 2 diabetes mellitus without complications"),
    ("J18.9", "Pneumonia, unspecified organism"),
    ("N17.9", "Acute kidney failure, unspecified"),
    ("Z96.64", "Presence of hip joint implant"),
    ("C18.9", "Malignant neoplasm of colon, unspecified"),
    ("I80.2", "Phlebitis and thrombophlebitis of deep vessels of lower extremities"),
    ("I48.91", "Unspecified atrial fibrillation"),
    ("A41.9", "Sepsis, unspecified organism"),
    ("J44.1", "COPD with acute exacerbation"),
    ("I16.0", "Hypertensive urgency"),
    ("C91.0", "Acute lymphoblastic leukaemia"),
    ("K35.2", "Acute appendicitis with generalised peritonitis"),
    ("K56.60", "Unspecified intestinal obstruction"),
]

DRUG_POOL = [
    ("Metoprolol", "50 mg", "Once daily"),
    ("Ramipril", "5 mg", "Once daily"),
    ("Prednisolone", "40 mg", "Once daily"),
    ("Salbutamol", "2.5 mg", "As needed"),
    ("Enoxaparin", "40 mg", "Once daily SC"),
    ("Paracetamol", "1 g", "Every 6 hours"),
    ("Aspirin", "300 mg", "Once daily"),
    ("Nimodipine", "60 mg", "Every 4 hours"),
    ("Metformin", "500 mg", "Twice daily"),
    ("Amoxicillin", "500 mg", "Three times daily"),
    ("Lisinopril", "10 mg", "Once daily"),
    ("Atorvastatin", "40 mg", "Once at night"),
    ("Furosemide", "40 mg", "Once daily"),
    ("Warfarin", "5 mg", "Once daily"),
    ("Omeprazole", "20 mg", "Once daily"),
    ("Morphine", "10 mg", "Every 4-6 hours PRN"),
    ("Dexamethasone", "8 mg", "Every 12 hours"),
    ("Piperacillin-Tazobactam", "4.5 g", "Every 8 hours IV"),
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
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def fmt_date(dt):
    return dt.strftime("%Y-%m-%d")


def fmt_dt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def rand_name(sex):
    first = random.choice(FIRST_NAMES_M if sex == "M" else FIRST_NAMES_F)
    last = random.choice(LAST_NAMES)
    return f"{first} {last}"


def chunk_inserts(rows, table, columns, chunk=500):
    # One row per INSERT — portable across PostgreSQL, MySQL,
    # MSSQL, Oracle, and DB2 (Oracle doesn't accept multi-row VALUES).
    return [f"INSERT INTO {table} ({columns}) VALUES ({r});" for r in rows]


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------


def gen_wards(n_wards):
    rows = []
    pool = WARD_LIST * ((n_wards // len(WARD_LIST)) + 2)
    for i, (name, floor, beds) in enumerate(pool[:n_wards], 1):
        rows.append({"ward_id": i, "name": name, "floor": floor, "total_beds": beds})
    return rows


def gen_staff(n, wards):
    rows = []
    roles_pool = STAFF_ROLES_POOL * ((n // len(STAFF_ROLES_POOL)) + 2)
    random.shuffle(roles_pool)
    for i in range(1, n + 1):
        role = roles_pool[i - 1]
        spec_list = SPECIALIZATIONS[role]
        spec = random.choice(spec_list)
        sex = random.choice(["M", "F"])
        name = rand_name(sex)
        last = name.split()[-1].lower()
        rows.append({
            "staff_id": i,
            "employee_no": f"EMP-{i:04d}",
            "name": (
                f"{'Dr.' if role in ('doctor', 'surgeon', 'radiologist') else ''} "
                f"{name}"
            ).strip(),
            "role": role,
            "specialization": spec,
            "phone": f"+44 20 7000 {random.randint(1000, 9999)}",
            "email": f"{last}{i}@city-hospital.nhs",
            "ward_id": random.choice(wards)["ward_id"]
            if random.random() < 0.85
            else None,
        })
    return rows


def gen_patients(n):
    rows = []
    birth_start = datetime(1940, 1, 1)
    birth_end = datetime(2010, 12, 31)
    for i in range(1, n + 1):
        sex = random.choice(["M", "F"])
        name = rand_name(sex)
        last = name.split()[-1].lower()
        dob = rand_date(birth_start, birth_end)
        bt = random.choices(BLOOD_TYPES, weights=BT_WEIGHTS)[0]
        n_all = random.choices([0, 1, 2], weights=[60, 30, 10])[0]
        allergies = ", ".join(random.sample(ALLERGY_LIST, n_all)) if n_all else None
        rows.append({
            "patient_id": i,
            "nhc": f"NHC-{100000 + i}",
            "name": name,
            "date_of_birth": fmt_date(dob),
            "sex": sex,
            "blood_type": bt,
            "phone": (
                f"+44 7{random.randint(100, 999)} {random.randint(100000, 999999)}"
            ),
            "email": f"{last}{i}@email.com",
            "address": (
                f"{random.randint(1, 200)} "
                f"{random.choice(['Oak', 'Maple', 'Pine', 'Elm', 'Birch', 'Cedar'])} "
                f"{random.choice(['St', 'Ave', 'Rd', 'Blvd', 'Lane'])}, "
                f"{random.choice(UK_CITIES)}"
            ),
            "emergency_contact_name": rand_name(random.choice(["M", "F"])),
            "emergency_contact_phone": (
                f"+44 7{random.randint(100, 999)} {random.randint(100000, 999999)}"
            ),
            "allergies": allergies,
        })
    return rows


def gen_admissions(n, patients, wards, doctors):
    rows = []
    adm_start = datetime(2023, 1, 1)
    adm_end = datetime(2024, 10, 31)
    for i in range(1, n + 1):
        pat = random.choice(patients)
        ward = random.choice(wards)
        doc = random.choice(doctors)
        adm = rand_date(adm_start, adm_end)
        discharged = None
        discharge_notes = None
        if random.random() < 0.85:
            stay = timedelta(days=random.randint(1, 21))
            discharged = adm + stay
            discharge_notes = random.choice([
                "Stable on discharge; GP follow-up arranged.",
                "Condition improved; outpatient review in 4 weeks.",
                "Surgical procedure successful; physiotherapy referral.",
                "Responded to treatment; continued oral medication.",
                "Palliative care plan established; community team informed.",
            ])
        rows.append({
            "admission_id": i,
            "patient_id": pat["patient_id"],
            "ward_id": ward["ward_id"],
            "attending_doctor_id": doc["staff_id"],
            "bed_number": f"{chr(65 + (i % 8))}-{random.randint(1, 20):02d}",
            "admitted_at": fmt_dt(adm),
            "discharged_at": fmt_dt(discharged) if discharged else None,
            "reason": random.choice(REASONS_FOR_ADMISSION),
            "discharge_notes": discharge_notes,
        })
    return rows


def gen_appointments(n, patients, doctors):
    rows = []
    appt_start = datetime(2023, 1, 1)
    appt_end = datetime(2025, 3, 31)
    types = ["consultation", "follow_up", "procedure", "emergency"]
    type_w = [30, 50, 15, 5]
    statuses = ["scheduled", "completed", "cancelled", "no_show"]
    status_w = [30, 55, 10, 5]
    for i in range(1, n + 1):
        pat = random.choice(patients)
        doc = random.choice(doctors)
        dt = rand_date(appt_start, appt_end)
        # Round to nearest 15 min
        dt = dt.replace(second=0, microsecond=0, minute=(dt.minute // 15) * 15)
        atype = random.choices(types, weights=type_w)[0]
        status = random.choices(statuses, weights=status_w)[0]
        rows.append({
            "appointment_id": i,
            "patient_id": pat["patient_id"],
            "doctor_id": doc["staff_id"],
            "scheduled_at": fmt_dt(dt),
            "duration_min": random.choice([15, 20, 30, 45, 60]),
            "type": atype,
            "status": status,
            "notes": None,
        })
    return sorted(rows, key=lambda r: r["scheduled_at"])


def gen_diagnoses(admissions, doctors):
    rows = []
    did = 1
    for adm in admissions:
        n_diag = random.choices([1, 2, 3], weights=[60, 30, 10])[0]
        samples = random.sample(ICD10_POOL, min(n_diag, len(ICD10_POOL)))
        adm_dt = datetime.strptime(adm["admitted_at"], "%Y-%m-%d %H:%M:%S")
        for code, desc in samples:
            diagnosed_at = adm_dt + timedelta(hours=random.randint(1, 8))
            rows.append({
                "diagnosis_id": did,
                "admission_id": adm["admission_id"],
                "icd10_code": code,
                "description": desc,
                "diagnosed_at": fmt_dt(diagnosed_at),
                "doctor_id": adm["attending_doctor_id"],
            })
            did += 1
    return rows


def gen_prescriptions(admissions, doctors):
    rows = []
    pid = 1
    for adm in admissions:
        n_rx = random.choices([0, 1, 2, 3, 4], weights=[10, 35, 30, 20, 5])[0]
        if n_rx == 0:
            continue
        samples = random.sample(DRUG_POOL, min(n_rx, len(DRUG_POOL)))
        adm_dt = datetime.strptime(adm["admitted_at"], "%Y-%m-%d %H:%M:%S")
        start = fmt_date(adm_dt)
        for drug, dosage, freq in samples:
            dur = random.choice([None, 5, 7, 14, 30, 90])
            end = None
            if dur:
                end = fmt_date(adm_dt + timedelta(days=dur))
            rows.append({
                "prescription_id": pid,
                "admission_id": adm["admission_id"],
                "doctor_id": adm["attending_doctor_id"],
                "drug_name": drug,
                "dosage": dosage,
                "frequency": freq,
                "start_date": start,
                "end_date": end,
                "notes": None,
            })
            pid += 1
    return rows


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

WARD_COLS = [
    Column("ward_id", "int", nullable=False, auto=True),
    Column("name", "varchar(100)", nullable=False),
    Column("floor", "smallint"),
    Column("total_beds", "smallint", nullable=False),
]
STAFF_COLS = [
    Column("staff_id", "int", nullable=False, auto=True),
    Column("employee_no", "varchar(20)", nullable=False),
    Column("name", "varchar(100)", nullable=False),
    Column("role", "varchar(20)", nullable=False),
    Column("specialization", "varchar(100)"),
    Column("phone", "varchar(30)"),
    Column("email", "varchar(255)"),
    Column("ward_id", "int"),
]
PAT_COLS = [
    Column("patient_id", "int", nullable=False, auto=True),
    Column("nhc", "varchar(20)", nullable=False),
    Column("name", "varchar(100)", nullable=False),
    Column("date_of_birth", "date", nullable=False),
    Column("sex", "char(1)", nullable=False),
    Column("blood_type", "varchar(3)"),
    Column("phone", "varchar(30)"),
    Column("email", "varchar(255)"),
    Column("address", "varchar(300)"),
    Column("emergency_contact_name", "varchar(100)"),
    Column("emergency_contact_phone", "varchar(30)"),
    Column("allergies", "text"),
]
ADM_COLS = [
    Column("admission_id", "int", nullable=False, auto=True),
    Column("patient_id", "int", nullable=False),
    Column("ward_id", "int", nullable=False),
    Column("attending_doctor_id", "int", nullable=False),
    Column("bed_number", "varchar(10)"),
    Column("admitted_at", "datetime", nullable=False),
    Column("discharged_at", "datetime"),
    Column("reason", "varchar(500)", nullable=False),
    Column("discharge_notes", "text"),
]
APPT_COLS = [
    Column("appointment_id", "int", nullable=False, auto=True),
    Column("patient_id", "int", nullable=False),
    Column("doctor_id", "int", nullable=False),
    Column("scheduled_at", "datetime", nullable=False),
    Column("duration_min", "smallint", nullable=False),
    Column("type", "varchar(20)", nullable=False),
    Column("status", "varchar(20)", nullable=False),
    Column("notes", "text"),
]
DIAG_COLS = [
    Column("diagnosis_id", "int", nullable=False, auto=True),
    Column("admission_id", "int", nullable=False),
    Column("icd10_code", "varchar(10)", nullable=False),
    Column("description", "varchar(300)", nullable=False),
    Column("diagnosed_at", "datetime", nullable=False),
    Column("doctor_id", "int", nullable=False),
]
RX_COLS = [
    Column("prescription_id", "int", nullable=False, auto=True),
    Column("admission_id", "int", nullable=False),
    Column("doctor_id", "int", nullable=False),
    Column("drug_name", "varchar(150)", nullable=False),
    Column("dosage", "varchar(100)", nullable=False),
    Column("frequency", "varchar(100)", nullable=False),
    Column("start_date", "date", nullable=False),
    Column("end_date", "date"),
    Column("notes", "text"),
]


def build_tables(args):
    random.seed(args.seed)
    print("Generating hospital data...", file=sys.stderr)
    wards = gen_wards(min(12, max(4, args.staff // 8)))
    staff = gen_staff(args.staff, wards)
    patients = gen_patients(args.patients)
    doctors = [s for s in staff if s["role"] in ("doctor", "surgeon", "radiologist")]
    if not doctors:
        doctors = staff[:1]
    admissions = gen_admissions(args.admissions, patients, wards, doctors)
    appointments = gen_appointments(args.appointments, patients, doctors)
    diagnoses = gen_diagnoses(admissions, doctors)
    prescriptions = gen_prescriptions(admissions, doctors)
    print(
        f"  wards={len(wards)} staff={len(staff)} patients={len(patients)} "
        f"admissions={len(admissions)} appointments={len(appointments)} "
        f"diagnoses={len(diagnoses)} prescriptions={len(prescriptions)}",
        file=sys.stderr,
    )

    return [
        Table("hp_wards", WARD_COLS, wards),
        Table("hp_staff", STAFF_COLS, staff),
        Table("hp_patients", PAT_COLS, patients),
        Table("hp_admissions", ADM_COLS, admissions),
        Table("hp_appointments", APPT_COLS, appointments),
        Table("hp_diagnoses", DIAG_COLS, diagnoses),
        Table("hp_prescriptions", RX_COLS, prescriptions),
    ]


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--patients", type=int, default=500)
    p.add_argument("--staff", type=int, default=80)
    p.add_argument("--admissions", type=int, default=600)
    p.add_argument("--appointments", type=int, default=1200)
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
