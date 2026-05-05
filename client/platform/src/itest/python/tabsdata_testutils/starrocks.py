#
# Copyright 2026 Tabs Data Inc.
#
"""StarRocks external system for live integration tests.

Gated by ``$STARROCKS`` — tests run unless ``$STARROCKS`` is set to
``"skip"`` (case-insensitive). When a ``prefix`` override is passed
to :func:`external`, the prefix replaces ``STARROCKS`` both as the
gate env var and as the base for every suffix below. StarRocks is
wire-protocol compatible with MySQL, so the bundle mirrors
:class:`MYSQL`. Conventional bundle that ``starrocks.sh`` exports:

* ``STARROCKS__HOST``      — hostname
* ``STARROCKS__PORT``      — TCP port (defaults to 9030 in the test itself)
* ``STARROCKS__DATABASE``  — database
* ``STARROCKS__USERNAME``  — user
* ``STARROCKS__PASSWORD``  — password
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


class STARROCKS(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "STARROCKS"

    @classmethod
    def env_names(cls) -> list[str]:
        return ["HOST", "PORT", "DATABASE", "USERNAME", "PASSWORD"]
