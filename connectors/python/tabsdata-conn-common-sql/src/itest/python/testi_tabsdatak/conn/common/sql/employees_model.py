#
# Copyright 2026 Tabsdata Inc.
#

"""The facts about the tdx ``employees`` model that the connectors' integration
tests assert against, in one place: every connector reads its provisioned
``<sys>_employees_xs`` domain, so none of them should be restating the model's
shape.

Row counts are not here -- those come from the descriptor via
:func:`.plugin_io.entity_rows`, so a domain seeded at another ``--size`` still
works.
"""

from __future__ import annotations

EMPLOYEES = "employees"
DEPARTMENTS = "departments"
DEPT_MANAGER = "dept_manager"

# Seeded employees are numbered from here, contiguously, so a row count is enough
# to know every emp_no in the domain.
FIRST_EMP_NO = 10001

EMPLOYEES_COLUMNS = {
    "emp_no",
    "birth_date",
    "first_name",
    "last_name",
    "gender",
    "hire_date",
}

# Fixed reference rows: they do not grow with the size multiplier.
DEPARTMENT_ROWS = {
    "d001": "Marketing",
    "d002": "Finance",
    "d003": "Human Resources",
    "d004": "Production",
    "d005": "Development",
    "d006": "Quality Management",
    "d007": "Sales",
    "d008": "Research",
    "d009": "Customer Service",
}


def emp_nos(rows: int) -> list[int]:
    """Every seeded ``emp_no``, for a domain holding `rows` employees."""
    return list(range(FIRST_EMP_NO, FIRST_EMP_NO + rows))
