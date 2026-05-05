#
# Copyright 2026 Tabs Data Inc.
#
"""Db2 CDC external system for live integration tests.

Gated by ``$CDCDB2`` — tests run unless ``$CDCDB2`` is set to
``"skip"`` (case-insensitive). When a ``prefix`` override is passed
to :func:`external`, the prefix replaces ``CDCDB2`` both as the
gate env var and as the base for every suffix below. The CDC gate
is kept distinct from the vanilla ``DB2`` gate because CDC needs a
specially configured instance, provisioned by a separate
``cdc_db2.sh`` script. Conventional bundle that ``cdc_db2.sh``
exports:

* ``CDCDB2__HOST``      — hostname
* ``CDCDB2__PORT``      — TCP port (defaults to 50000 in the test itself)
* ``CDCDB2__DATABASE``  — database
* ``CDCDB2__SCHEMA``    — optional schema
* ``CDCDB2__USERNAME``  — user
* ``CDCDB2__PASSWORD``  — password
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


class CDCDB2(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "CDCDB2"

    @classmethod
    def env_names(cls) -> list[str]:
        return ["HOST", "PORT", "DATABASE", "SCHEMA", "USERNAME", "PASSWORD"]
