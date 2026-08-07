#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.conn.gcs.error import GCSErrorCode

# --- Project ---------------------------------------------------------

# A GCP project id has no shape worth enforcing locally -- Google rejects
# an id it does not own -- so the only rule is that it is not empty.
PROJECT_VALIDATION = _api.ValueConstraints(
    _api.Rule.length(GCSErrorCode.GCS_11, min_len=1)
)
