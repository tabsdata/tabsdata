-- =============================================================
-- Oracle DDL — Employees
-- =============================================================

CREATE TABLE departments (
    dept_no         CHAR(4)         PRIMARY KEY,
    dept_name       VARCHAR2(40)    NOT NULL UNIQUE
);

CREATE TABLE employees (
    emp_no          NUMBER(10)      PRIMARY KEY,
    birth_date      DATE            NOT NULL,
    first_name      VARCHAR2(14)    NOT NULL,
    last_name       VARCHAR2(16)    NOT NULL,
    gender          CHAR(1)         NOT NULL CHECK (gender IN ('M','F')),
    hire_date       DATE            NOT NULL
);

CREATE TABLE dept_emp (
    emp_no          NUMBER(10)      NOT NULL REFERENCES employees   (emp_no)  ON DELETE CASCADE,
    dept_no         CHAR(4)         NOT NULL REFERENCES departments (dept_no) ON DELETE CASCADE,
    from_date       DATE            NOT NULL,
    to_date         DATE            NOT NULL,
    PRIMARY KEY (emp_no, dept_no)
);
CREATE INDEX ix_empde_dept ON dept_emp (dept_no);

CREATE TABLE dept_manager (
    emp_no          NUMBER(10)      NOT NULL REFERENCES employees   (emp_no)  ON DELETE CASCADE,
    dept_no         CHAR(4)         NOT NULL REFERENCES departments (dept_no) ON DELETE CASCADE,
    from_date       DATE            NOT NULL,
    to_date         DATE            NOT NULL,
    PRIMARY KEY (emp_no, dept_no)
);
CREATE INDEX ix_empdm_dept ON dept_manager (dept_no);

CREATE TABLE titles (
    emp_no          NUMBER(10)      NOT NULL REFERENCES employees (emp_no) ON DELETE CASCADE,
    title           VARCHAR2(50)    NOT NULL,
    from_date       DATE            NOT NULL,
    to_date         DATE,
    PRIMARY KEY (emp_no, title, from_date)
);

CREATE TABLE salaries (
    emp_no          NUMBER(10)      NOT NULL REFERENCES employees (emp_no) ON DELETE CASCADE,
    salary          NUMBER(10)      NOT NULL,
    from_date       DATE            NOT NULL,
    to_date         DATE            NOT NULL,
    PRIMARY KEY (emp_no, from_date)
);
