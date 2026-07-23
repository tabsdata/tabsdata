-- =============================================================
-- Oracle DDL — Real-World Domain Schemas
-- Domain: Hospital Patient
-- =============================================================

CREATE TABLE wards (
    ward_id         NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            VARCHAR2(100)   NOT NULL,
    floor           NUMBER(5),
    total_beds      NUMBER(5)       NOT NULL CHECK (total_beds > 0)
);

CREATE TABLE staff (
    staff_id        NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    employee_no     VARCHAR2(20)    NOT NULL UNIQUE,
    name            VARCHAR2(100)   NOT NULL,
    role            VARCHAR2(30)    NOT NULL CHECK (role IN ('doctor','nurse','surgeon','radiologist','admin','pharmacist')),
    specialization  VARCHAR2(100),
    phone           VARCHAR2(30),
    email           VARCHAR2(255),
    ward_id         NUMBER          REFERENCES wards (ward_id) ON DELETE SET NULL
);

CREATE TABLE patients (
    patient_id      NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nhc             VARCHAR2(20)    NOT NULL UNIQUE,
    name            VARCHAR2(100)   NOT NULL,
    date_of_birth   DATE            NOT NULL,
    sex             CHAR(1)         NOT NULL CHECK (sex IN ('M','F','O')),
    blood_type      VARCHAR2(3)     CHECK (blood_type IN ('A+','A-','B+','B-','AB+','AB-','O+','O-')),
    phone           VARCHAR2(30),
    email           VARCHAR2(255),
    address         VARCHAR2(300),
    emergency_contact_name  VARCHAR2(100),
    emergency_contact_phone VARCHAR2(30),
    allergies       CLOB
);

CREATE TABLE admissions (
    admission_id    NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    patient_id      NUMBER          NOT NULL REFERENCES patients (patient_id),
    ward_id         NUMBER          NOT NULL REFERENCES wards    (ward_id),
    attending_doctor_id NUMBER      NOT NULL REFERENCES staff    (staff_id),
    bed_number      VARCHAR2(10),
    admitted_at     TIMESTAMP WITH TIME ZONE NOT NULL,
    discharged_at   TIMESTAMP WITH TIME ZONE,
    reason          VARCHAR2(500)   NOT NULL,
    discharge_notes CLOB
);

CREATE TABLE appointments (
    appointment_id  NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    patient_id      NUMBER          NOT NULL REFERENCES patients (patient_id),
    doctor_id       NUMBER          NOT NULL REFERENCES staff    (staff_id),
    scheduled_at    TIMESTAMP WITH TIME ZONE NOT NULL,
    duration_min    NUMBER(5)       DEFAULT 20 NOT NULL,
    type            VARCHAR2(20)    NOT NULL CHECK (type IN ('consultation','follow_up','procedure','emergency')),
    status          VARCHAR2(20)    DEFAULT 'scheduled' NOT NULL
                    CHECK (status IN ('scheduled','completed','cancelled','no_show')),
    notes           CLOB
);

CREATE TABLE diagnoses (
    diagnosis_id    NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    admission_id    NUMBER          NOT NULL REFERENCES admissions (admission_id) ON DELETE CASCADE,
    icd10_code      VARCHAR2(10)    NOT NULL,
    description     VARCHAR2(300)   NOT NULL,
    diagnosed_at    TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    doctor_id       NUMBER          NOT NULL REFERENCES staff (staff_id)
);

CREATE TABLE prescriptions (
    prescription_id NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    admission_id    NUMBER          NOT NULL REFERENCES admissions (admission_id) ON DELETE CASCADE,
    doctor_id       NUMBER          NOT NULL REFERENCES staff      (staff_id),
    drug_name       VARCHAR2(150)   NOT NULL,
    dosage          VARCHAR2(100)   NOT NULL,
    frequency       VARCHAR2(100)   NOT NULL,
    start_date      DATE            NOT NULL,
    end_date        DATE,
    notes           CLOB
);
