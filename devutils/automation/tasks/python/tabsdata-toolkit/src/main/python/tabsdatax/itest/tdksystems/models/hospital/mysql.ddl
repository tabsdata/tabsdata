-- =============================================================
-- MySQL DDL — Real-World Domain Schemas
-- Domains: Supermarket Inventory, Hospital Patient, Insurance
--          Management, Insurance Claims, Courier Logistics,
--          School Grades, Restaurant Menu & Provisioning
-- =============================================================

-- =============================================================
-- 2. HOSPITAL PATIENT SYSTEM
-- =============================================================

CREATE TABLE hp_wards (
    ward_id         INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    name            VARCHAR(100)    NOT NULL,
    floor           TINYINT UNSIGNED,
    total_beds      SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY (ward_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE hp_staff (
    staff_id        INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    employee_no     VARCHAR(20)     NOT NULL,
    name            VARCHAR(100)    NOT NULL,
    role            ENUM('doctor','nurse','surgeon','radiologist','admin','pharmacist') NOT NULL,
    specialization  VARCHAR(100),
    phone           VARCHAR(30),
    email           VARCHAR(255),
    ward_id         INT UNSIGNED,
    PRIMARY KEY (staff_id),
    UNIQUE KEY uq_hp_empno (employee_no),
    CONSTRAINT fk_hpstaff_ward FOREIGN KEY (ward_id) REFERENCES hp_wards (ward_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE hp_patients (
    patient_id      INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    nhc             VARCHAR(20)     NOT NULL,   -- National Health Card number
    name            VARCHAR(100)    NOT NULL,
    date_of_birth   DATE            NOT NULL,
    sex             ENUM('M','F','O') NOT NULL,
    blood_type      ENUM('A+','A-','B+','B-','AB+','AB-','O+','O-'),
    phone           VARCHAR(30),
    email           VARCHAR(255),
    address         VARCHAR(300),
    emergency_contact_name  VARCHAR(100),
    emergency_contact_phone VARCHAR(30),
    allergies       TEXT,
    PRIMARY KEY (patient_id),
    UNIQUE KEY uq_hp_nhc (nhc)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE hp_admissions (
    admission_id    INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    patient_id      INT UNSIGNED    NOT NULL,
    ward_id         INT UNSIGNED    NOT NULL,
    attending_doctor_id INT UNSIGNED NOT NULL,
    bed_number      VARCHAR(10),
    admitted_at     DATETIME        NOT NULL,
    discharged_at   DATETIME,
    reason          VARCHAR(500)    NOT NULL,
    discharge_notes TEXT,
    PRIMARY KEY (admission_id),
    CONSTRAINT fk_hpadm_pat  FOREIGN KEY (patient_id)         REFERENCES hp_patients (patient_id) ON DELETE RESTRICT,
    CONSTRAINT fk_hpadm_ward FOREIGN KEY (ward_id)            REFERENCES hp_wards    (ward_id)    ON DELETE RESTRICT,
    CONSTRAINT fk_hpadm_doc  FOREIGN KEY (attending_doctor_id) REFERENCES hp_staff   (staff_id)   ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE hp_appointments (
    appointment_id  INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    patient_id      INT UNSIGNED    NOT NULL,
    doctor_id       INT UNSIGNED    NOT NULL,
    scheduled_at    DATETIME        NOT NULL,
    duration_min    SMALLINT UNSIGNED NOT NULL DEFAULT 20,
    type            ENUM('consultation','follow_up','procedure','emergency') NOT NULL,
    status          ENUM('scheduled','completed','cancelled','no_show') NOT NULL DEFAULT 'scheduled',
    notes           TEXT,
    PRIMARY KEY (appointment_id),
    CONSTRAINT fk_hpappt_pat FOREIGN KEY (patient_id) REFERENCES hp_patients (patient_id) ON DELETE RESTRICT,
    CONSTRAINT fk_hpappt_doc FOREIGN KEY (doctor_id)  REFERENCES hp_staff    (staff_id)   ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE hp_diagnoses (
    diagnosis_id    INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    admission_id    INT UNSIGNED    NOT NULL,
    icd10_code      VARCHAR(10)     NOT NULL,
    description     VARCHAR(300)    NOT NULL,
    diagnosed_at    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    doctor_id       INT UNSIGNED    NOT NULL,
    PRIMARY KEY (diagnosis_id),
    CONSTRAINT fk_hpdiag_adm FOREIGN KEY (admission_id) REFERENCES hp_admissions (admission_id) ON DELETE CASCADE,
    CONSTRAINT fk_hpdiag_doc FOREIGN KEY (doctor_id)    REFERENCES hp_staff       (staff_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE hp_prescriptions (
    prescription_id INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    admission_id    INT UNSIGNED    NOT NULL,
    doctor_id       INT UNSIGNED    NOT NULL,
    drug_name       VARCHAR(150)    NOT NULL,
    dosage          VARCHAR(100)    NOT NULL,
    frequency       VARCHAR(100)    NOT NULL,
    start_date      DATE            NOT NULL,
    end_date        DATE,
    notes           TEXT,
    PRIMARY KEY (prescription_id),
    CONSTRAINT fk_hprx_adm FOREIGN KEY (admission_id) REFERENCES hp_admissions  (admission_id) ON DELETE CASCADE,
    CONSTRAINT fk_hprx_doc FOREIGN KEY (doctor_id)    REFERENCES hp_staff        (staff_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
