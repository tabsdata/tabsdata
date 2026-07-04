-- =============================================================
-- Oracle DDL — Real-World Domain Schemas
-- Domain: School Classes and Student Grades
-- =============================================================

CREATE TABLE sch_academic_years (
    year_id     NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    label       VARCHAR2(20)    NOT NULL,
    start_date  DATE            NOT NULL,
    end_date    DATE            NOT NULL
);

CREATE TABLE sch_departments (
    dept_id     NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        VARCHAR2(100)   NOT NULL
);

CREATE TABLE sch_teachers (
    teacher_id      NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    employee_no     VARCHAR2(20)    NOT NULL UNIQUE,
    name            VARCHAR2(100)   NOT NULL,
    email           VARCHAR2(255)   NOT NULL,
    dept_id         NUMBER          REFERENCES sch_departments (dept_id) ON DELETE SET NULL
);

CREATE TABLE sch_subjects (
    subject_id  NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    code        VARCHAR2(10)    NOT NULL UNIQUE,
    name        VARCHAR2(150)   NOT NULL,
    credits     NUMBER(5)       DEFAULT 3 NOT NULL CHECK (credits > 0),
    dept_id     NUMBER          REFERENCES sch_departments (dept_id) ON DELETE SET NULL
);

CREATE TABLE sch_classes (
    class_id        NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subject_id      NUMBER          NOT NULL REFERENCES sch_subjects       (subject_id),
    teacher_id      NUMBER          NOT NULL REFERENCES sch_teachers       (teacher_id),
    year_id         NUMBER          NOT NULL REFERENCES sch_academic_years (year_id),
    section         VARCHAR2(5)     DEFAULT 'A' NOT NULL,
    schedule        VARCHAR2(100),
    room            VARCHAR2(20),
    max_students    NUMBER(5)       DEFAULT 30 NOT NULL
);

CREATE TABLE sch_students (
    student_id      NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    student_no      VARCHAR2(15)    NOT NULL UNIQUE,
    name            VARCHAR2(100)   NOT NULL,
    date_of_birth   DATE            NOT NULL,
    email           VARCHAR2(255),
    enrolled_year_id NUMBER         NOT NULL REFERENCES sch_academic_years (year_id)
);

CREATE TABLE sch_enrollments (
    enrollment_id   NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    student_id      NUMBER          NOT NULL REFERENCES sch_students (student_id) ON DELETE CASCADE,
    class_id        NUMBER          NOT NULL REFERENCES sch_classes  (class_id),
    enrolled_on     DATE            DEFAULT TRUNC(CURRENT_DATE) NOT NULL,
    CONSTRAINT uq_sch_enrollment UNIQUE (student_id, class_id)
);

CREATE TABLE sch_assessments (
    assessment_id   NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    class_id        NUMBER          NOT NULL REFERENCES sch_classes (class_id) ON DELETE CASCADE,
    name            VARCHAR2(100)   NOT NULL,
    type            VARCHAR2(20)    NOT NULL CHECK (type IN ('exam','test','assignment','lab','project','oral')),
    weight          NUMBER(5,2)     NOT NULL CHECK (weight > 0 AND weight <= 100),
    max_score       NUMBER(6,2)     DEFAULT 100.00 NOT NULL,
    due_date        DATE
);

CREATE TABLE sch_grades (
    grade_id        NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    enrollment_id   NUMBER          NOT NULL REFERENCES sch_enrollments (enrollment_id) ON DELETE CASCADE,
    assessment_id   NUMBER          NOT NULL REFERENCES sch_assessments  (assessment_id),
    score           NUMBER(6,2)     NOT NULL CHECK (score >= 0),
    graded_at       TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    notes           VARCHAR2(300),
    CONSTRAINT uq_sch_grade UNIQUE (enrollment_id, assessment_id)
);
