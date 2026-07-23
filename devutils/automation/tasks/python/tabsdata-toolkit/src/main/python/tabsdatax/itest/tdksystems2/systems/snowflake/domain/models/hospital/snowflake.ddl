-- =============================================================
-- Snowflake SQL DDL — Hospital Patient System
-- Targets a Snowflake schema (database.schema).
-- PK/FK constraints are informational on Snowflake; we keep
-- AUTOINCREMENT identity columns and index-free DDL (no CREATE INDEX).
-- =============================================================

CREATE TABLE wards (
    ward_id         BIGINT          AUTOINCREMENT,
    name            STRING          NOT NULL,
    floor           TINYINT,
    total_beds      SMALLINT        NOT NULL
);

CREATE TABLE staff (
    staff_id        BIGINT          AUTOINCREMENT,
    employee_no     STRING          NOT NULL,
    name            STRING          NOT NULL,
    role            STRING          NOT NULL,
    specialization  STRING,
    phone           STRING,
    email           STRING,
    ward_id         INT
);

CREATE TABLE patients (
    patient_id      BIGINT          AUTOINCREMENT,
    nhc             STRING          NOT NULL,
    name            STRING          NOT NULL,
    date_of_birth   DATE            NOT NULL,
    sex             STRING          NOT NULL,
    blood_type      STRING,
    phone           STRING,
    email           STRING,
    address         STRING,
    emergency_contact_name  STRING,
    emergency_contact_phone STRING,
    allergies       STRING
);

CREATE TABLE admissions (
    admission_id    BIGINT          AUTOINCREMENT,
    patient_id      INT             NOT NULL,
    ward_id         INT             NOT NULL,
    attending_doctor_id INT         NOT NULL,
    bed_number      STRING,
    admitted_at     TIMESTAMP       NOT NULL,
    discharged_at   TIMESTAMP,
    reason          STRING          NOT NULL,
    discharge_notes STRING
);

CREATE TABLE appointments (
    appointment_id  BIGINT          AUTOINCREMENT,
    patient_id      INT             NOT NULL,
    doctor_id       INT             NOT NULL,
    scheduled_at    TIMESTAMP       NOT NULL,
    duration_min    SMALLINT        NOT NULL,
    type            STRING          NOT NULL,
    status          STRING          NOT NULL,
    notes           STRING
);

CREATE TABLE diagnoses (
    diagnosis_id    BIGINT          AUTOINCREMENT,
    admission_id    INT             NOT NULL,
    icd10_code      STRING          NOT NULL,
    description     STRING          NOT NULL,
    diagnosed_at    TIMESTAMP       NOT NULL,
    doctor_id       INT             NOT NULL
);

CREATE TABLE prescriptions (
    prescription_id BIGINT          AUTOINCREMENT,
    admission_id    INT             NOT NULL,
    doctor_id       INT             NOT NULL,
    drug_name       STRING          NOT NULL,
    dosage          STRING          NOT NULL,
    frequency       STRING          NOT NULL,
    start_date      DATE            NOT NULL,
    end_date        DATE,
    notes           STRING
);
