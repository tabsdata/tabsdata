-- =============================================================
-- MySQL DDL — Real-World Domain Schemas
-- Domains: Supermarket Inventory, Hospital Patient, Insurance
--          Management, Insurance Claims, Courier Logistics,
--          School Grades, Restaurant Menu & Provisioning
-- =============================================================

-- =============================================================
-- 6. SCHOOL CLASSES AND STUDENT GRADES SYSTEM
-- =============================================================

CREATE TABLE academic_years (
    year_id         INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    label           VARCHAR(20)     NOT NULL,   -- e.g. '2023-24'
    start_date      DATE            NOT NULL,
    end_date        DATE            NOT NULL,
    PRIMARY KEY (year_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE departments (
    dept_id     INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    name        VARCHAR(100)    NOT NULL,
    PRIMARY KEY (dept_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE teachers (
    teacher_id      INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    employee_no     VARCHAR(20)     NOT NULL,
    name            VARCHAR(100)    NOT NULL,
    email           VARCHAR(255)    NOT NULL,
    dept_id         INT UNSIGNED,
    PRIMARY KEY (teacher_id),
    UNIQUE KEY uq_sch_teacher_emp (employee_no),
    CONSTRAINT fk_schteach_dept FOREIGN KEY (dept_id) REFERENCES departments (dept_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE subjects (
    subject_id      INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    code            VARCHAR(10)     NOT NULL,
    name            VARCHAR(150)    NOT NULL,
    credits         TINYINT UNSIGNED NOT NULL DEFAULT 3,
    dept_id         INT UNSIGNED,
    PRIMARY KEY (subject_id),
    UNIQUE KEY uq_sch_subj_code (code),
    CONSTRAINT fk_schsubj_dept FOREIGN KEY (dept_id) REFERENCES departments (dept_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE classes (
    class_id        INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    subject_id      INT UNSIGNED    NOT NULL,
    teacher_id      INT UNSIGNED    NOT NULL,
    year_id         INT UNSIGNED    NOT NULL,
    section         VARCHAR(5)      NOT NULL DEFAULT 'A',
    schedule        VARCHAR(100),
    room            VARCHAR(20),
    max_students    TINYINT UNSIGNED NOT NULL DEFAULT 30,
    PRIMARY KEY (class_id),
    CONSTRAINT fk_schcls_subj    FOREIGN KEY (subject_id) REFERENCES subjects      (subject_id),
    CONSTRAINT fk_schcls_teacher FOREIGN KEY (teacher_id) REFERENCES teachers      (teacher_id),
    CONSTRAINT fk_schcls_year    FOREIGN KEY (year_id)    REFERENCES academic_years (year_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE students (
    student_id      INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    student_no      VARCHAR(15)     NOT NULL,
    name            VARCHAR(100)    NOT NULL,
    date_of_birth   DATE            NOT NULL,
    email           VARCHAR(255),
    enrolled_year_id INT UNSIGNED   NOT NULL,
    PRIMARY KEY (student_id),
    UNIQUE KEY uq_sch_stuno (student_no),
    CONSTRAINT fk_schstu_year FOREIGN KEY (enrolled_year_id) REFERENCES academic_years (year_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE enrollments (
    enrollment_id   INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    student_id      INT UNSIGNED    NOT NULL,
    class_id        INT UNSIGNED    NOT NULL,
    enrolled_on     DATE            NOT NULL DEFAULT (CURRENT_DATE),
    PRIMARY KEY (enrollment_id),
    UNIQUE KEY uq_sch_enroll (student_id, class_id),
    CONSTRAINT fk_schenr_stu FOREIGN KEY (student_id) REFERENCES students (student_id) ON DELETE CASCADE,
    CONSTRAINT fk_schenr_cls FOREIGN KEY (class_id)   REFERENCES classes  (class_id)   ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE assessments (
    assessment_id   INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    class_id        INT UNSIGNED    NOT NULL,
    name            VARCHAR(100)    NOT NULL,   -- 'Midterm Exam', 'Lab Report 1'
    type            ENUM('exam','test','assignment','lab','project','oral') NOT NULL,
    weight          DECIMAL(5,2)    NOT NULL,   -- % contribution to final grade
    max_score       DECIMAL(6,2)    NOT NULL DEFAULT 100.00,
    due_date        DATE,
    PRIMARY KEY (assessment_id),
    CONSTRAINT fk_schasm_cls FOREIGN KEY (class_id) REFERENCES classes (class_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE grades (
    grade_id        INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    enrollment_id   INT UNSIGNED    NOT NULL,
    assessment_id   INT UNSIGNED    NOT NULL,
    score           DECIMAL(6,2)    NOT NULL,
    graded_at       DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    notes           VARCHAR(300),
    PRIMARY KEY (grade_id),
    UNIQUE KEY uq_sch_grade (enrollment_id, assessment_id),
    CONSTRAINT fk_schgrd_enr FOREIGN KEY (enrollment_id) REFERENCES enrollments (enrollment_id) ON DELETE CASCADE,
    CONSTRAINT fk_schgrd_asm FOREIGN KEY (assessment_id) REFERENCES assessments  (assessment_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
