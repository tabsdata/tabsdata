#
# Copyright 2026 Tabs Data Inc.
#
"""PostgreSQL external system for live integration tests.

Gated by ``$POSTGRES`` — tests run unless ``$POSTGRES`` is set to
``"skip"`` (case-insensitive). When a ``prefix`` override is passed
to :func:`external`, the prefix replaces ``POSTGRES`` both as the
gate env var and as the base for every suffix below. Conventional
bundle that ``postgres.sh`` exports:

* ``POSTGRES__HOST``      — hostname
* ``POSTGRES__PORT``      — TCP port (defaults to 5432 in the test itself)
* ``POSTGRES__DATABASE``  — database to connect to
* ``POSTGRES__SCHEMA``    — optional schema; the extractor scopes to it when set
* ``POSTGRES__USERNAME``  — user
* ``POSTGRES__PASSWORD``  — password
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


class POSTGRES(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "POSTGRES"

    @classmethod
    def env_names(cls) -> list[str]:
        return ["HOST", "PORT", "DATABASE", "SCHEMA", "USERNAME", "PASSWORD"]
