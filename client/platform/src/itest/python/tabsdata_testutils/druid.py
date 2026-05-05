#
# Copyright 2026 Tabs Data Inc.
#
"""Apache Druid external system for live integration tests.

Gated by ``$DRUID`` — tests run unless ``$DRUID`` is set to
``"skip"`` (case-insensitive). When a ``prefix`` override is passed
to :func:`external`, the prefix replaces ``DRUID`` both as the gate
env var and as the base for every suffix below. Conventional bundle
that ``druid.sh`` exports:

* ``DRUID__HOST``      — coordinator hostname
* ``DRUID__PORT``      — TCP port (defaults to 8082 in the test itself)
* ``DRUID__PATH``      — optional URL path suffix under the coordinator
* ``DRUID__TLS``       — ``1``/``true``/``yes`` switches the scheme to ``https``
* ``DRUID__USERNAME``  — user
* ``DRUID__PASSWORD``  — password
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


class DRUID(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "DRUID"

    @classmethod
    def env_names(cls) -> list[str]:
        return ["HOST", "PORT", "PATH", "TLS", "USERNAME", "PASSWORD"]
