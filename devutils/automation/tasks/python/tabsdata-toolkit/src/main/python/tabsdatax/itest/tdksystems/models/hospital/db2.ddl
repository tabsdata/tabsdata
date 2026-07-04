-- =============================================================
-- IBM Db2 DDL — Real-World Domain Schemas
-- Domain: Hospital Patient
-- =============================================================

CREATE TABLE hp_wards (
    ward_id         INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    name            VARCHAR(100)    NOT NULL,
    floor           SMALLINT,
    total_beds      SMALLINT        NOT NULL CHECK (total_beds > 0)
);

CREATE TABLE hp_staff (
    staff_id        INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    employee_no     VARCHAR(20)     NOT NULL UNIQUE,
    name            VARCHAR(100)    NOT NULL,
    role            VARCHAR(30)     NOT NULL CHECK (role IN ('doctor','nurse','surgeon','radiologist','admin','pharmacist')),
    specialization  VARCHAR(100),
    phone           VARCHAR(30),
    email           VARCHAR(255),
    ward_id         INTEGER         REFERENCES hp_wards (ward_id) ON DELETE SET NULL
);

CREATE TABLE hp_patients (
    patient_id      INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    nhc             VARCHAR(20)     NOT NULL UNIQUE,
    name            VARCHAR(100)    NOT NULL,
    date_of_birth   DATE            NOT NULL,
    sex             CHAR(1)         NOT NULL CHECK (sex IN ('M','F','O')),
    blood_type      VARCHAR(3)      CHECK (blood_type IN ('A+','A-','B+','B-','AB+','AB-','O+','O-')),
    phone           VARCHAR(30),
    email           VARCHAR(255),
    address         VARCHAR(300),
    emergency_contact_name  VARCHAR(100),
    emergency_contact_phone VARCHAR(30),
    allergies       CLOB
);

CREATE TABLE hp_admissions (
    admission_id    INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    patient_id      INTEGER         NOT NULL REFERENCES hp_patients (patient_id),
    ward_id         INTEGER         NOT NULL REFERENCES hp_wards    (ward_id),
    attending_doctor_id INTEGER     NOT NULL REFERENCES hp_staff    (staff_id),
    bed_number      VARCHAR(10),
    admitted_at     TIMESTAMP       NOT NULL,
    discharged_at   TIMESTAMP,
    reason          VARCHAR(500)    NOT NULL,
    discharge_notes CLOB
);

CREATE TABLE hp_appointments (
    appointment_id  INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    patient_id      INTEGER         NOT NULL REFERENCES hp_patients (patient_id),
    doctor_id       INTEGER         NOT NULL REFERENCES hp_staff    (staff_id),
    scheduled_at    TIMESTAMP       NOT NULL,
    duration_min    SMALLINT        NOT NULL DEFAULT 20,
    type            VARCHAR(20)     NOT NULL CHECK (type IN ('consultation','follow_up','procedure','emergency')),
    status          VARCHAR(20)     NOT NULL DEFAULT 'scheduled'
                    CHECK (status IN ('scheduled','completed','cancelled','no_show')),
    notes           CLOB
);

CREATE TABLE hp_diagnoses (
    diagnosis_id    INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    admission_id    INTEGER         NOT NULL REFERENCES hp_admissions (admission_id) ON DELETE CASCADE,
    icd10_code      VARCHAR(10)     NOT NULL,
    description     VARCHAR(300)    NOT NULL,
    diagnosed_at    TIMESTAMP       NOT NULL DEFAULT CURRENT TIMESTAMP,
    doctor_id       INTEGER         NOT NULL REFERENCES hp_staff (staff_id)
);

CREATE TABLE hp_prescriptions (
    prescription_id INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    admission_id    INTEGER         NOT NULL REFERENCES hp_admissions (admission_id) ON DELETE CASCADE,
    doctor_id       INTEGER         NOT NULL REFERENCES hp_staff      (staff_id),
    drug_name       VARCHAR(150)    NOT NULL,
    dosage          VARCHAR(100)    NOT NULL,
    frequency       VARCHAR(100)    NOT NULL,
    start_date      DATE            NOT NULL,
    end_date        DATE,
    notes           CLOB
);
