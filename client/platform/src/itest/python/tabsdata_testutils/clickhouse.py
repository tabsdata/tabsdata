#
# Copyright 2026 Tabs Data Inc.
#
"""ClickHouse external system for live integration tests.

Gated by ``$CLICKHOUSE`` — tests run unless ``$CLICKHOUSE`` is set
to ``"skip"`` (case-insensitive). When a ``prefix`` override is
passed to :func:`external`, the prefix replaces ``CLICKHOUSE`` both
as the gate env var and as the base for every suffix below.
Conventional bundle that ``clickhouse.sh`` exports:

* ``CLICKHOUSE__HOST``      — hostname
* ``CLICKHOUSE__PORT``      — TCP port (defaults to 9000 in the test itself)
* ``CLICKHOUSE__DATABASE``  — database
* ``CLICKHOUSE__USERNAME``  — user
* ``CLICKHOUSE__PASSWORD``  — password
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


class CLICKHOUSE(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "CLICKHOUSE"

    @classmethod
    def env_names(cls) -> list[str]:
        return ["HOST", "PORT", "DATABASE", "USERNAME", "PASSWORD"]
