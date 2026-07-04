-- =============================================================
-- MySQL DDL — Employees (HR system: departments, employees,
-- assignments, titles, salary history)
-- =============================================================

CREATE TABLE emp_departments (
    dept_no         CHAR(4)         NOT NULL,
    dept_name       VARCHAR(40)     NOT NULL,
    PRIMARY KEY (dept_no),
    UNIQUE KEY uq_emp_deptname (dept_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE emp_employees (
    emp_no          INT             NOT NULL,
    birth_date      DATE            NOT NULL,
    first_name      VARCHAR(14)     NOT NULL,
    last_name       VARCHAR(16)     NOT NULL,
    gender          ENUM('M','F')   NOT NULL,
    hire_date       DATE            NOT NULL,
    PRIMARY KEY (emp_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE emp_dept_emp (
    emp_no          INT             NOT NULL,
    dept_no         CHAR(4)         NOT NULL,
    from_date       DATE            NOT NULL,
    to_date         DATE            NOT NULL,
    PRIMARY KEY (emp_no, dept_no),
    KEY ix_empde_dept (dept_no),
    CONSTRAINT fk_empde_emp  FOREIGN KEY (emp_no)  REFERENCES emp_employees   (emp_no)  ON DELETE CASCADE,
    CONSTRAINT fk_empde_dept FOREIGN KEY (dept_no) REFERENCES emp_departments (dept_no) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE emp_dept_manager (
    emp_no          INT             NOT NULL,
    dept_no         CHAR(4)         NOT NULL,
    from_date       DATE            NOT NULL,
    to_date         DATE            NOT NULL,
    PRIMARY KEY (emp_no, dept_no),
    KEY ix_empdm_dept (dept_no),
    CONSTRAINT fk_empdm_emp  FOREIGN KEY (emp_no)  REFERENCES emp_employees   (emp_no)  ON DELETE CASCADE,
    CONSTRAINT fk_empdm_dept FOREIGN KEY (dept_no) REFERENCES emp_departments (dept_no) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE emp_titles (
    emp_no          INT             NOT NULL,
    title           VARCHAR(50)     NOT NULL,
    from_date       DATE            NOT NULL,
    to_date         DATE,
    PRIMARY KEY (emp_no, title, from_date),
    CONSTRAINT fk_emptit_emp FOREIGN KEY (emp_no) REFERENCES emp_employees (emp_no) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE emp_salaries (
    emp_no          INT             NOT NULL,
    salary          INT             NOT NULL,
    from_date       DATE            NOT NULL,
    to_date         DATE            NOT NULL,
    PRIMARY KEY (emp_no, from_date),
    CONSTRAINT fk_empsal_emp FOREIGN KEY (emp_no) REFERENCES emp_employees (emp_no) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
