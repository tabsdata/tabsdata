#
# Copyright 2026 Tabs Data Inc.
#
"""MySQL CDC external system for live integration tests.

Gated by ``$CDCMYSQL`` — tests run unless ``$CDCMYSQL`` is set to
``"skip"`` (case-insensitive). When a ``prefix`` override is passed
to :func:`external`, the prefix replaces ``CDCMYSQL`` both as the
gate env var and as the base for every suffix below. The CDC gate
is kept distinct from the vanilla ``MYSQL`` gate because CDC needs
a specially configured instance (binlog enabled), provisioned by a
separate ``cdc_mysql.sh`` script. Conventional bundle that
``cdc_mysql.sh`` exports:

* ``CDCMYSQL__HOST``      — hostname
* ``CDCMYSQL__PORT``      — TCP port (defaults to 3306 in the test itself)
* ``CDCMYSQL__DATABASE``  — database
* ``CDCMYSQL__USERNAME``  — user
* ``CDCMYSQL__PASSWORD``  — password
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


class CDCMYSQL(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "CDCMYSQL"

    @classmethod
    def env_names(cls) -> list[str]:
        return ["HOST", "PORT", "DATABASE", "USERNAME", "PASSWORD"]
