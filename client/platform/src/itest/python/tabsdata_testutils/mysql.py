#
# Copyright 2026 Tabs Data Inc.
#
"""MySQL external system for live integration tests.

Gated by ``$MYSQL`` — tests run unless ``$MYSQL`` is set to
``"skip"`` (case-insensitive). When a ``prefix`` override is passed
to :func:`external`, the prefix replaces ``MYSQL`` both as the gate
env var and as the base for every suffix below. Conventional bundle
that ``mysql.sh`` exports:

* ``MYSQL__HOST``      — hostname
* ``MYSQL__PORT``      — TCP port (defaults to 3306 in the test itself)
* ``MYSQL__DATABASE``  — database to connect to
* ``MYSQL__USERNAME``  — user
* ``MYSQL__PASSWORD``  — password
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


# Class name matches the gate env var so ``@external(system=MYSQL)``
# reads fluidly at the call site. PEP-8 class-case is intentionally
# traded for call-site clarity here.
class MYSQL(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "MYSQL"

    @classmethod
    def env_names(cls) -> list[str]:
        return ["HOST", "PORT", "DATABASE", "USERNAME", "PASSWORD"]
