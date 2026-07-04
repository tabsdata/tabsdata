-- =============================================================
-- Databricks SQL DDL — School Grades
-- Targets a Unity-Catalog schema; tables default to Delta.
-- PK/FK constraints are informational on Databricks; IDENTITY
-- columns kept, no CREATE INDEX.
-- =============================================================

CREATE TABLE sch_academic_years (
    year_id         BIGINT          GENERATED ALWAYS AS IDENTITY,
    label           STRING          NOT NULL,
    start_date      DATE            NOT NULL,
    end_date        DATE            NOT NULL
);

CREATE TABLE sch_departments (
    dept_id         BIGINT          GENERATED ALWAYS AS IDENTITY,
    name            STRING          NOT NULL
);

CREATE TABLE sch_teachers (
    teacher_id      BIGINT          GENERATED ALWAYS AS IDENTITY,
    employee_no     STRING          NOT NULL,
    name            STRING          NOT NULL,
    email           STRING          NOT NULL,
    dept_id         INT
);

CREATE TABLE sch_subjects (
    subject_id      BIGINT          GENERATED ALWAYS AS IDENTITY,
    code            STRING          NOT NULL,
    name            STRING          NOT NULL,
    credits         TINYINT         NOT NULL,
    dept_id         INT
);

CREATE TABLE sch_classes (
    class_id        BIGINT          GENERATED ALWAYS AS IDENTITY,
    subject_id      INT             NOT NULL,
    teacher_id      INT             NOT NULL,
    year_id         INT             NOT NULL,
    section         STRING          NOT NULL,
    schedule        STRING,
    room            STRING,
    max_students    TINYINT         NOT NULL
);

CREATE TABLE sch_students (
    student_id          BIGINT      GENERATED ALWAYS AS IDENTITY,
    student_no          STRING      NOT NULL,
    name                STRING      NOT NULL,
    date_of_birth       DATE        NOT NULL,
    email               STRING,
    enrolled_year_id    INT         NOT NULL
);

CREATE TABLE sch_enrollments (
    enrollment_id   BIGINT          GENERATED ALWAYS AS IDENTITY,
    student_id      INT             NOT NULL,
    class_id        INT             NOT NULL,
    enrolled_on     DATE            NOT NULL
);

CREATE TABLE sch_assessments (
    assessment_id   BIGINT          GENERATED ALWAYS AS IDENTITY,
    class_id        INT             NOT NULL,
    name            STRING          NOT NULL,
    type            STRING          NOT NULL,
    weight          DECIMAL(5,2)    NOT NULL,
    max_score       DECIMAL(6,2)    NOT NULL,
    due_date        DATE
);

CREATE TABLE sch_grades (
    grade_id        BIGINT          GENERATED ALWAYS AS IDENTITY,
    enrollment_id   INT             NOT NULL,
    assessment_id   INT             NOT NULL,
    score           DECIMAL(6,2)    NOT NULL,
    graded_at       TIMESTAMP       NOT NULL,
    notes           STRING
);
