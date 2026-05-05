#
# Copyright 2026 Tabs Data Inc.
#
"""Google Cloud Storage external system for live integration tests.

Gated by ``$GCS`` — tests run unless ``$GCS`` is set to ``"skip"``
(case-insensitive). When a ``prefix`` override is passed to
:func:`external`, the prefix replaces ``GCS`` both as the gate env
var and as the base for every suffix below. Conventional bundle
that ``gcs.sh`` exports:

* ``GCS__PROJECT``           — GCP project ID
* ``GCS__BUCKET``            — bucket name
* ``GCS__CREDENTIALS_FILE``  — path to the service-account JSON key
* ``GCS__CREDENTIALS_JSON``  — inline service-account JSON (preferred over _FILE)
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


class GCS(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "GCS"

    @classmethod
    def env_names(cls) -> list[str]:
        return ["PROJECT", "BUCKET", "CREDENTIALS_FILE", "CREDENTIALS_JSON"]
