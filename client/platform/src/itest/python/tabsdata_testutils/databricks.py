#
# Copyright 2026 Tabs Data Inc.
#
"""Databricks external system for live integration tests.

Gated by ``$DATABRICKS`` — tests run unless ``$DATABRICKS`` is set
to ``"skip"`` (case-insensitive). When a ``prefix`` override is
passed to :func:`external`, the prefix replaces ``DATABRICKS`` both
as the gate env var and as the base for every suffix below.
Conventional bundle that ``databricks.sh`` exports:

* ``DATABRICKS__HOST``     — workspace URL
* ``DATABRICKS__TOKEN``    — personal access token (PAT-based auth)
* ``DATABRICKS__CATALOG``  — optional Unity Catalog catalog
* ``DATABRICKS__SCHEMA``   — optional schema
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


class DATABRICKS(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "DATABRICKS"

    @classmethod
    def env_names(cls) -> list[str]:
        return ["HOST", "TOKEN", "CATALOG", "SCHEMA"]
