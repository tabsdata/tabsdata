-- =============================================================
-- Snowflake SQL DDL — Employees (HR system)
-- Targets a Snowflake schema (database.schema).
-- PK/FK constraints are informational on Snowflake; we keep
-- AUTOINCREMENT identity columns and index-free DDL (no CREATE INDEX).
-- =============================================================

CREATE TABLE departments (
    dept_no         STRING          NOT NULL,
    dept_name       STRING          NOT NULL
);

CREATE TABLE employees (
    emp_no          INT             NOT NULL,
    birth_date      DATE            NOT NULL,
    first_name      STRING          NOT NULL,
    last_name       STRING          NOT NULL,
    gender          STRING          NOT NULL,
    hire_date       DATE            NOT NULL
);

CREATE TABLE dept_emp (
    emp_no          INT             NOT NULL,
    dept_no         STRING          NOT NULL,
    from_date       DATE            NOT NULL,
    to_date         DATE            NOT NULL
);

CREATE TABLE dept_manager (
    emp_no          INT             NOT NULL,
    dept_no         STRING          NOT NULL,
    from_date       DATE            NOT NULL,
    to_date         DATE            NOT NULL
);

CREATE TABLE titles (
    emp_no          INT             NOT NULL,
    title           STRING          NOT NULL,
    from_date       DATE            NOT NULL,
    to_date         DATE
);

CREATE TABLE salaries (
    emp_no          INT             NOT NULL,
    salary          INT             NOT NULL,
    from_date       DATE            NOT NULL,
    to_date         DATE            NOT NULL
);
