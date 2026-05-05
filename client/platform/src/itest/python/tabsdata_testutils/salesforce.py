#
# Copyright 2026 Tabs Data Inc.
#
"""Salesforce external system for live integration tests.

Gated by ``$SALESFORCE`` — tests run unless ``$SALESFORCE`` is set
to ``"skip"`` (case-insensitive). When a ``prefix`` override is
passed to :func:`external`, the prefix replaces ``SALESFORCE`` both
as the gate env var and as the base for every suffix below. One
Salesforce org powers both the SOQL and the Reports extractors —
they share this bundle. Conventional bundle that ``salesforce.sh``
exports:

* ``SALESFORCE__USERNAME``        — Salesforce login
* ``SALESFORCE__PASSWORD``        — password
* ``SALESFORCE__SECURITY_TOKEN``  — security token appended to the password
* ``SALESFORCE__INSTANCE_URL``    — optional instance URL
  (required when the login binds to multiple instances)
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


class SALESFORCE(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "SALESFORCE"

    @classmethod
    def env_names(cls) -> list[str]:
        return ["USERNAME", "PASSWORD", "SECURITY_TOKEN", "INSTANCE_URL"]
