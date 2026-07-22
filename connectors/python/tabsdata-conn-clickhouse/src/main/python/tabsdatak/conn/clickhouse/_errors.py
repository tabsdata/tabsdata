#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations


class SchemaError(Exception):
    """Raised when table schema does not match the expected schema."""


class BulkLoadError(Exception):
    """Raised when a bulk load operation fails."""
