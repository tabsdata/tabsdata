#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Publish an Oracle query
kind: publisher
connectors: [oracle-sql-in]
tags: [sql, query, uppercase-columns]
summary: >-
  Publishes an Oracle query result. Oracle folds unquoted identifiers to
  uppercase, so the function renames the columns to lowercase.
"""

from tabsdatak.api import TableFrameSpec, publisher
from tabsdatak.conn.oracle import OracleSrc


@publisher(
    source=OracleSrc(
        queries=["SELECT id, name, department, salary FROM hr.employees ORDER BY id"],
    ),
    output_tables=["employees"],
)
def pub_oracle_query(employees: TableFrameSpec) -> TableFrameSpec:
    if employees is None:
        return None
    return employees.rename({
        "ID": "id",
        "NAME": "name",
        "DEPARTMENT": "department",
        "SALARY": "salary",
    })
