#
# Copyright 2026 Tabs Data Inc.
#
"""MongoDB external system for live integration tests.

Gated by ``$MONGODB`` — tests run unless ``$MONGODB`` is set to
``"skip"`` (case-insensitive). When a ``prefix`` override is passed
to :func:`external`, the prefix replaces ``MONGODB`` both as the
gate env var and as the base for every suffix below. Conventional
bundle that ``mongodb.sh`` exports:

* ``MONGODB__URI``       — full pre-assembled URI (wins over the component fields)
* ``MONGODB__HOST``      — hostname (used when no URI is set)
* ``MONGODB__PORT``      — TCP port (defaults to 27017 in the test itself)
* ``MONGODB__DATABASE``  — database
* ``MONGODB__USERNAME``  — user
* ``MONGODB__PASSWORD``  — password
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


class MONGODB(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "MONGODB"

    @classmethod
    def env_names(cls) -> list[str]:
        return ["URI", "HOST", "PORT", "DATABASE", "USERNAME", "PASSWORD"]
