#
# Copyright 2026 Tabs Data Inc.
#
"""Oracle external system for live integration tests.

Gated by ``$ORACLE`` — tests run unless ``$ORACLE`` is set to
``"skip"`` (case-insensitive). When a ``prefix`` override is passed
to :func:`external`, the prefix replaces ``ORACLE`` both as the gate
env var and as the base for every suffix below. Conventional bundle
that ``oracle.sh`` exports:

* ``ORACLE__HOST``          — hostname
* ``ORACLE__PORT``          — TCP port (defaults to 1521 in the test itself)
* ``ORACLE__DATABASE``      — SID to connect to
* ``ORACLE__SERVICE_NAME``  — optional; when set, connect by service name instead of SID
* ``ORACLE__OWNER``         — optional schema owner
  (distinct from the connecting user on Oracle)
* ``ORACLE__USERNAME``      — user
* ``ORACLE__PASSWORD``      — password
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


class ORACLE(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "ORACLE"

    @classmethod
    def env_names(cls) -> list[str]:
        return [
            "HOST",
            "PORT",
            "DATABASE",
            "SERVICE_NAME",
            "OWNER",
            "USERNAME",
            "PASSWORD",
        ]
