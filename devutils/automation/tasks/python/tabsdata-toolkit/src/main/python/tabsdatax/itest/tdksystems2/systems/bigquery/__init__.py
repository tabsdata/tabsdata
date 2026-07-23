#
# Copyright 2026 Tabsdata Inc.
#

# BigQuery system: a GCP dataset + GCS staging prefix per domain (stage-only, no
# data plane). Only the backend + create command are system-specific; every other
# verb runs the shared command in _common/commands.

from tabsdatax.itest.tdksystems2.systems.bigquery.backend import Bigquery

__all__ = ["Bigquery"]
