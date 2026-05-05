#
# Copyright 2026 Tabs Data Inc.
#
"""IBM Db2 external system for live integration tests.

Gated by ``$DB2`` — tests run unless ``$DB2`` is set to ``"skip"``
(case-insensitive). When a ``prefix`` override is passed to
:func:`external`, the prefix replaces ``DB2`` both as the gate env
var and as the base for every suffix below. Conventional bundle
that ``db2.sh`` exports:

* ``DB2__HOST``      — hostname
* ``DB2__PORT``      — TCP port (defaults to 50000 in the test itself)
* ``DB2__DATABASE``  — database to connect to
* ``DB2__SCHEMA``    — optional schema; the extractor scopes to it when set
* ``DB2__USERNAME``  — user
* ``DB2__PASSWORD``  — password
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


class DB2(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "DB2"

    @classmethod
    def env_names(cls) -> list[str]:
        return ["HOST", "PORT", "DATABASE", "SCHEMA", "USERNAME", "PASSWORD"]
