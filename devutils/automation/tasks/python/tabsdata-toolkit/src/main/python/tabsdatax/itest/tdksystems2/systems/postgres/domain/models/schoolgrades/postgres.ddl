-- =============================================================
-- PostgreSQL DDL — Real-World Domain Schemas
-- Domains: Supermarket Inventory, Hospital Patient, Insurance
--          Management, Insurance Claims, Courier Logistics,
--          School Grades, Restaurant Menu & Provisioning
-- =============================================================

-- =============================================================
-- 6. SCHOOL CLASSES AND STUDENT GRADES SYSTEM
-- =============================================================

CREATE TABLE academic_years (
    year_id     SERIAL          PRIMARY KEY,
    label       VARCHAR(20)     NOT NULL,
    start_date  DATE            NOT NULL,
    end_date    DATE            NOT NULL
);

CREATE TABLE departments (
    dept_id     SERIAL          PRIMARY KEY,
    name        VARCHAR(100)    NOT NULL
);

CREATE TABLE teachers (
    teacher_id      SERIAL          PRIMARY KEY,
    employee_no     VARCHAR(20)     NOT NULL UNIQUE,
    name            VARCHAR(100)    NOT NULL,
    email           VARCHAR(255)    NOT NULL,
    dept_id         INT             REFERENCES departments (dept_id) ON DELETE SET NULL
);

CREATE TABLE subjects (
    subject_id  SERIAL          PRIMARY KEY,
    code        VARCHAR(10)     NOT NULL UNIQUE,
    name        VARCHAR(150)    NOT NULL,
    credits     SMALLINT        NOT NULL DEFAULT 3 CHECK (credits > 0),
    dept_id     INT             REFERENCES departments (dept_id) ON DELETE SET NULL
);

CREATE TABLE classes (
    class_id        SERIAL          PRIMARY KEY,
    subject_id      INT             NOT NULL REFERENCES subjects      (subject_id),
    teacher_id      INT             NOT NULL REFERENCES teachers      (teacher_id),
    year_id         INT             NOT NULL REFERENCES academic_years (year_id),
    section         VARCHAR(5)      NOT NULL DEFAULT 'A',
    schedule        VARCHAR(100),
    room            VARCHAR(20),
    max_students    SMALLINT        NOT NULL DEFAULT 30
);

CREATE TABLE students (
    student_id      SERIAL          PRIMARY KEY,
    student_no      VARCHAR(15)     NOT NULL UNIQUE,
    name            VARCHAR(100)    NOT NULL,
    date_of_birth   DATE            NOT NULL,
    email           VARCHAR(255),
    enrolled_year_id INT            NOT NULL REFERENCES academic_years (year_id)
);

CREATE TABLE enrollments (
    enrollment_id   SERIAL          PRIMARY KEY,
    student_id      INT             NOT NULL REFERENCES students (student_id) ON DELETE CASCADE,
    class_id        INT             NOT NULL REFERENCES classes  (class_id)   ON DELETE RESTRICT,
    enrolled_on     DATE            NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (student_id, class_id)
);

CREATE TABLE assessments (
    assessment_id   SERIAL          PRIMARY KEY,
    class_id        INT             NOT NULL REFERENCES classes (class_id) ON DELETE CASCADE,
    name            VARCHAR(100)    NOT NULL,
    type            TEXT            NOT NULL CHECK (type IN ('exam','test','assignment','lab','project','oral')),
    weight          NUMERIC(5,2)    NOT NULL CHECK (weight > 0 AND weight <= 100),
    max_score       NUMERIC(6,2)    NOT NULL DEFAULT 100.00,
    due_date        DATE
);

CREATE TABLE grades (
    grade_id        SERIAL          PRIMARY KEY,
    enrollment_id   INT             NOT NULL REFERENCES enrollments (enrollment_id) ON DELETE CASCADE,
    assessment_id   INT             NOT NULL REFERENCES assessments  (assessment_id) ON DELETE CASCADE,
    score           NUMERIC(6,2)    NOT NULL CHECK (score >= 0),
    graded_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    notes           VARCHAR(300),
    UNIQUE (enrollment_id, assessment_id)
);
