#
# Copyright 2026 Tabs Data Inc.
#
"""Google BigQuery external system for live integration tests.

Gated by ``$BIGQUERY`` — tests run unless ``$BIGQUERY`` is set to
``"skip"`` (case-insensitive). When a ``prefix`` override is passed
to :func:`external`, the prefix replaces ``BIGQUERY`` both as the
gate env var and as the base for every suffix below. Conventional
bundle that ``bigquery.sh`` exports:

* ``BIGQUERY__PROJECT``           — GCP project ID
* ``BIGQUERY__DATASET``           — dataset the extractor reads
* ``BIGQUERY__CREDENTIALS_FILE``  — path to the service-account JSON key
* ``BIGQUERY__CREDENTIALS_JSON``  — inline service-account JSON (preferred over _FILE)
* ``BIGQUERY__GCS_FOLDER``        — optional GCS staging folder for federated queries
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


class BIGQUERY(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "BIGQUERY"

    @classmethod
    def env_names(cls) -> list[str]:
        return [
            "PROJECT",
            "DATASET",
            "CREDENTIALS_FILE",
            "CREDENTIALS_JSON",
            "GCS_FOLDER",
        ]
