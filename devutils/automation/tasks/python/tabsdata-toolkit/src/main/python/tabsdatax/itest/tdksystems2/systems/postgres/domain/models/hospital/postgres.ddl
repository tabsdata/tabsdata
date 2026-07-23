-- =============================================================
-- PostgreSQL DDL — Real-World Domain Schemas
-- Domains: Supermarket Inventory, Hospital Patient, Insurance
--          Management, Insurance Claims, Courier Logistics,
--          School Grades, Restaurant Menu & Provisioning
-- =============================================================

-- =============================================================
-- 2. HOSPITAL PATIENT SYSTEM
-- =============================================================

CREATE TABLE wards (
    ward_id         SERIAL          PRIMARY KEY,
    name            VARCHAR(100)    NOT NULL,
    floor           SMALLINT,
    total_beds      SMALLINT        NOT NULL CHECK (total_beds > 0)
);

CREATE TABLE staff (
    staff_id        SERIAL          PRIMARY KEY,
    employee_no     VARCHAR(20)     NOT NULL UNIQUE,
    name            VARCHAR(100)    NOT NULL,
    role            TEXT            NOT NULL CHECK (role IN ('doctor','nurse','surgeon','radiologist','admin','pharmacist')),
    specialization  VARCHAR(100),
    phone           VARCHAR(30),
    email           VARCHAR(255),
    ward_id         INT             REFERENCES wards (ward_id) ON DELETE SET NULL
);

CREATE TABLE patients (
    patient_id      SERIAL          PRIMARY KEY,
    nhc             VARCHAR(20)     NOT NULL UNIQUE,
    name            VARCHAR(100)    NOT NULL,
    date_of_birth   DATE            NOT NULL,
    sex             CHAR(1)         NOT NULL CHECK (sex IN ('M','F','O')),
    blood_type      TEXT            CHECK (blood_type IN ('A+','A-','B+','B-','AB+','AB-','O+','O-')),
    phone           VARCHAR(30),
    email           VARCHAR(255),
    address         VARCHAR(300),
    emergency_contact_name  VARCHAR(100),
    emergency_contact_phone VARCHAR(30),
    allergies       TEXT
);

CREATE TABLE admissions (
    admission_id    SERIAL          PRIMARY KEY,
    patient_id      INT             NOT NULL REFERENCES patients (patient_id) ON DELETE RESTRICT,
    ward_id         INT             NOT NULL REFERENCES wards    (ward_id)    ON DELETE RESTRICT,
    attending_doctor_id INT         NOT NULL REFERENCES staff    (staff_id)   ON DELETE RESTRICT,
    bed_number      VARCHAR(10),
    admitted_at     TIMESTAMPTZ     NOT NULL,
    discharged_at   TIMESTAMPTZ,
    reason          VARCHAR(500)    NOT NULL,
    discharge_notes TEXT
);

CREATE TABLE appointments (
    appointment_id  SERIAL          PRIMARY KEY,
    patient_id      INT             NOT NULL REFERENCES patients (patient_id) ON DELETE RESTRICT,
    doctor_id       INT             NOT NULL REFERENCES staff    (staff_id)   ON DELETE RESTRICT,
    scheduled_at    TIMESTAMPTZ     NOT NULL,
    duration_min    SMALLINT        NOT NULL DEFAULT 20,
    type            TEXT            NOT NULL CHECK (type IN ('consultation','follow_up','procedure','emergency')),
    status          TEXT            NOT NULL DEFAULT 'scheduled'
                    CHECK (status IN ('scheduled','completed','cancelled','no_show')),
    notes           TEXT
);

CREATE TABLE diagnoses (
    diagnosis_id    SERIAL          PRIMARY KEY,
    admission_id    INT             NOT NULL REFERENCES admissions (admission_id) ON DELETE CASCADE,
    icd10_code      VARCHAR(10)     NOT NULL,
    description     VARCHAR(300)    NOT NULL,
    diagnosed_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    doctor_id       INT             NOT NULL REFERENCES staff (staff_id)
);

CREATE TABLE prescriptions (
    prescription_id SERIAL          PRIMARY KEY,
    admission_id    INT             NOT NULL REFERENCES admissions (admission_id) ON DELETE CASCADE,
    doctor_id       INT             NOT NULL REFERENCES staff      (staff_id),
    drug_name       VARCHAR(150)    NOT NULL,
    dosage          VARCHAR(100)    NOT NULL,
    frequency       VARCHAR(100)    NOT NULL,
    start_date      DATE            NOT NULL,
    end_date        DATE,
    notes           TEXT
);
