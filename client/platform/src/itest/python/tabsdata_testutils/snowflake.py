#
# Copyright 2026 Tabs Data Inc.
#
"""Snowflake external system for live integration tests.

Gated by ``$SNOWFLAKE`` — tests run unless ``$SNOWFLAKE`` is set to
``"skip"`` (case-insensitive). When a ``prefix`` override is passed
to :func:`external`, the prefix replaces ``SNOWFLAKE`` both as the
gate env var and as the base for every suffix below. Conventional
bundle that ``snowflake.sh`` exports:

* ``SNOWFLAKE__ACCOUNT``    — tenant identifier
* ``SNOWFLAKE__USERNAME``   — user
* ``SNOWFLAKE__PASSWORD``   — password
* ``SNOWFLAKE__DATABASE``   — database
* ``SNOWFLAKE__SCHEMA``     — schema
* ``SNOWFLAKE__ROLE``       — Snowflake role
* ``SNOWFLAKE__WAREHOUSE``  — compute warehouse
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


class SNOWFLAKE(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "SNOWFLAKE"

    @classmethod
    def env_names(cls) -> list[str]:
        return [
            "ACCOUNT",
            "USERNAME",
            "PASSWORD",
            "DATABASE",
            "SCHEMA",
            "ROLE",
            "WAREHOUSE",
        ]
