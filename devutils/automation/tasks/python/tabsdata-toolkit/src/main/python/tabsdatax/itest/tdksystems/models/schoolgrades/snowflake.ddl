-- =============================================================
-- Snowflake SQL DDL — School Grades
-- Targets a Snowflake schema (database.schema).
-- PK/FK constraints are informational on Snowflake; we keep
-- AUTOINCREMENT identity columns and index-free DDL (no CREATE INDEX).
-- =============================================================

CREATE TABLE sch_academic_years (
    year_id         BIGINT          AUTOINCREMENT,
    label           STRING          NOT NULL,
    start_date      DATE            NOT NULL,
    end_date        DATE            NOT NULL
);

CREATE TABLE sch_departments (
    dept_id         BIGINT          AUTOINCREMENT,
    name            STRING          NOT NULL
);

CREATE TABLE sch_teachers (
    teacher_id      BIGINT          AUTOINCREMENT,
    employee_no     STRING          NOT NULL,
    name            STRING          NOT NULL,
    email           STRING          NOT NULL,
    dept_id         INT
);

CREATE TABLE sch_subjects (
    subject_id      BIGINT          AUTOINCREMENT,
    code            STRING          NOT NULL,
    name            STRING          NOT NULL,
    credits         TINYINT         NOT NULL,
    dept_id         INT
);

CREATE TABLE sch_classes (
    class_id        BIGINT          AUTOINCREMENT,
    subject_id      INT             NOT NULL,
    teacher_id      INT             NOT NULL,
    year_id         INT             NOT NULL,
    section         STRING          NOT NULL,
    schedule        STRING,
    room            STRING,
    max_students    TINYINT         NOT NULL
);

CREATE TABLE sch_students (
    student_id          BIGINT      AUTOINCREMENT,
    student_no          STRING      NOT NULL,
    name                STRING      NOT NULL,
    date_of_birth       DATE        NOT NULL,
    email               STRING,
    enrolled_year_id    INT         NOT NULL
);

CREATE TABLE sch_enrollments (
    enrollment_id   BIGINT          AUTOINCREMENT,
    student_id      INT             NOT NULL,
    class_id        INT             NOT NULL,
    enrolled_on     DATE            NOT NULL
);

CREATE TABLE sch_assessments (
    assessment_id   BIGINT          AUTOINCREMENT,
    class_id        INT             NOT NULL,
    name            STRING          NOT NULL,
    type            STRING          NOT NULL,
    weight          DECIMAL(5,2)    NOT NULL,
    max_score       DECIMAL(6,2)    NOT NULL,
    due_date        DATE
);

CREATE TABLE sch_grades (
    grade_id        BIGINT          AUTOINCREMENT,
    enrollment_id   INT             NOT NULL,
    assessment_id   INT             NOT NULL,
    score           DECIMAL(6,2)    NOT NULL,
    graded_at       TIMESTAMP       NOT NULL,
    notes           STRING
);
