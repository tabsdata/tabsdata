-- =============================================================
-- Microsoft SQL Server DDL — Real-World Domain Schemas
-- Domain: School Classes and Student Grades
-- =============================================================

CREATE TABLE sch_academic_years (
    year_id     INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    label       VARCHAR(20)     NOT NULL,
    start_date  DATE            NOT NULL,
    end_date    DATE            NOT NULL
);

CREATE TABLE sch_departments (
    dept_id     INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    name        VARCHAR(100)    NOT NULL
);

CREATE TABLE sch_teachers (
    teacher_id      INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    employee_no     VARCHAR(20)     NOT NULL UNIQUE,
    name            VARCHAR(100)    NOT NULL,
    email           VARCHAR(255)    NOT NULL,
    dept_id         INT             REFERENCES sch_departments (dept_id) ON DELETE SET NULL
);

CREATE TABLE sch_subjects (
    subject_id  INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    code        VARCHAR(10)     NOT NULL UNIQUE,
    name        VARCHAR(150)    NOT NULL,
    credits     SMALLINT        NOT NULL DEFAULT 3 CHECK (credits > 0),
    dept_id     INT             REFERENCES sch_departments (dept_id) ON DELETE SET NULL
);

CREATE TABLE sch_classes (
    class_id        INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    subject_id      INT             NOT NULL REFERENCES sch_subjects       (subject_id),
    teacher_id      INT             NOT NULL REFERENCES sch_teachers       (teacher_id),
    year_id         INT             NOT NULL REFERENCES sch_academic_years (year_id),
    section         VARCHAR(5)      NOT NULL DEFAULT 'A',
    schedule        VARCHAR(100),
    room            VARCHAR(20),
    max_students    SMALLINT        NOT NULL DEFAULT 30
);

CREATE TABLE sch_students (
    student_id      INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    student_no      VARCHAR(15)     NOT NULL UNIQUE,
    name            VARCHAR(100)    NOT NULL,
    date_of_birth   DATE            NOT NULL,
    email           VARCHAR(255),
    enrolled_year_id INT            NOT NULL REFERENCES sch_academic_years (year_id)
);

CREATE TABLE sch_enrollments (
    enrollment_id   INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    student_id      INT             NOT NULL REFERENCES sch_students (student_id) ON DELETE CASCADE,
    class_id        INT             NOT NULL REFERENCES sch_classes  (class_id),
    enrolled_on     DATE            NOT NULL DEFAULT CAST(GETDATE() AS DATE),
    CONSTRAINT uq_sch_enrollment UNIQUE (student_id, class_id)
);

CREATE TABLE sch_assessments (
    assessment_id   INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    class_id        INT             NOT NULL REFERENCES sch_classes (class_id) ON DELETE CASCADE,
    name            VARCHAR(100)    NOT NULL,
    type            VARCHAR(20)     NOT NULL CHECK (type IN ('exam','test','assignment','lab','project','oral')),
    weight          DECIMAL(5,2)    NOT NULL CHECK (weight > 0 AND weight <= 100),
    max_score       DECIMAL(6,2)    NOT NULL DEFAULT 100.00,
    due_date        DATE
);

CREATE TABLE sch_grades (
    grade_id        INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    enrollment_id   INT             NOT NULL REFERENCES sch_enrollments (enrollment_id) ON DELETE CASCADE,
    assessment_id   INT             NOT NULL REFERENCES sch_assessments  (assessment_id),
    score           DECIMAL(6,2)    NOT NULL CHECK (score >= 0),
    graded_at       DATETIMEOFFSET  NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    notes           VARCHAR(300),
    CONSTRAINT uq_sch_grade UNIQUE (enrollment_id, assessment_id)
);
