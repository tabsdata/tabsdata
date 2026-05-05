#
# Copyright 2026 Tabs Data Inc.
#
"""Microsoft SQL Server external system for live integration tests.

Gated by ``$MSSQL`` — tests run unless ``$MSSQL`` is set to
``"skip"`` (case-insensitive). When a ``prefix`` override is passed
to :func:`external`, the prefix replaces ``MSSQL`` both as the gate
env var and as the base for every suffix below. Conventional bundle
that ``mssql.sh`` exports:

* ``MSSQL__HOST``                — hostname
* ``MSSQL__PORT``                — TCP port (defaults to 1433 in the test itself)
* ``MSSQL__DATABASE``            — database to connect to
* ``MSSQL__SCHEMA``              — optional schema; the extractor scopes to it when set
* ``MSSQL__USERNAME``            — user
* ``MSSQL__PASSWORD``            — password
* ``MSSQL__DRIVER``              — optional ODBC driver name for pyodbc
* ``MSSQL__TRUSTED_CONNECTION``  — ``1``/``true`` to use integrated auth
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


class MSSQL(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "MSSQL"

    @classmethod
    def env_names(cls) -> list[str]:
        return [
            "HOST",
            "PORT",
            "DATABASE",
            "SCHEMA",
            "USERNAME",
            "PASSWORD",
            "DRIVER",
            "TRUSTED_CONNECTION",
        ]
