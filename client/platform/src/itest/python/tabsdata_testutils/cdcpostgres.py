#
# Copyright 2026 Tabs Data Inc.
#
"""Postgres CDC external system for live integration tests.

Gated by ``$CDCPOSTGRES`` — tests run unless ``$CDCPOSTGRES`` is set
to ``"skip"`` (case-insensitive). When a ``prefix`` override is
passed to :func:`external`, the prefix replaces ``CDCPOSTGRES``
both as the gate env var and as the base for every suffix below.
The CDC gate is kept distinct from the vanilla ``POSTGRES`` gate
because CDC needs a specially configured instance (logical
replication enabled), provisioned by a separate
``cdc_postgres.sh`` script. Conventional bundle that
``cdc_postgres.sh`` exports:

* ``CDCPOSTGRES__HOST``      — hostname
* ``CDCPOSTGRES__PORT``      — TCP port (defaults to 5432 in the test itself)
* ``CDCPOSTGRES__DATABASE``  — database
* ``CDCPOSTGRES__SCHEMA``    — optional schema
* ``CDCPOSTGRES__USERNAME``  — replication-enabled user
* ``CDCPOSTGRES__PASSWORD``  — password
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


class CDCPOSTGRES(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "CDCPOSTGRES"

    @classmethod
    def env_names(cls) -> list[str]:
        return ["HOST", "PORT", "DATABASE", "SCHEMA", "USERNAME", "PASSWORD"]
