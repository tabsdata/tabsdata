#
# Copyright 2026 Tabs Data Inc.
#
"""Azure Blob Storage external system for live integration tests.

Gated by ``$AZURE`` — tests run unless ``$AZURE`` is set to
``"skip"`` (case-insensitive). When a ``prefix`` override is passed
to :func:`external`, the prefix replaces ``AZURE`` both as the gate
env var and as the base for every suffix below. Supports either the
three-part tuple or a single pre-built connection string — tests
pick whichever is populated. Conventional bundle that ``azure.sh``
exports:

* ``AZURE__ACCOUNT``            — storage account name
* ``AZURE__ACCOUNT_KEY``        — storage account key
* ``AZURE__CONTAINER``          — container name
* ``AZURE__CONNECTION_STRING``  — optional pre-built connection string (wins when set)
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


class AZURE(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "AZURE"

    @classmethod
    def env_names(cls) -> list[str]:
        return ["ACCOUNT", "ACCOUNT_KEY", "CONTAINER", "CONNECTION_STRING"]
