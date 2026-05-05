#
# Copyright 2026 Tabs Data Inc.
#
"""MariaDB external system for live integration tests.

Gated by ``$MARIADB`` — tests run unless ``$MARIADB`` is set to
``"skip"`` (case-insensitive). When a ``prefix`` override is passed
to :func:`external`, the prefix replaces ``MARIADB`` both as the
gate env var and as the base for every suffix below. Conventional
bundle that ``mariadb.sh`` exports:

* ``MARIADB__HOST``      — hostname
* ``MARIADB__PORT``      — TCP port (defaults to 3306 in the test itself)
* ``MARIADB__DATABASE``  — database to connect to
* ``MARIADB__USERNAME``  — user
* ``MARIADB__PASSWORD``  — password
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


class MARIADB(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "MARIADB"

    @classmethod
    def env_names(cls) -> list[str]:
        return ["HOST", "PORT", "DATABASE", "USERNAME", "PASSWORD"]
