#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path


class CloudStorage(ABC):
    """Abstract interface for cloud storage operations.

    Implementations may return physical URIs (S3, Azure, GCS) or logical paths
    (e.g. Snowflake stage references). The dialect must understand both forms.
    """

    @abstractmethod
    def upload(self, upload_id: str, local_paths: list[Path]) -> list[str]:
        """Upload files to storage. Returns a list of storage URIs (or logical paths),
        one per input file, in the same order as local_paths."""

    @abstractmethod
    def delete(self, up_to_id: str) -> None:
        """Delete all staged files with upload_id <= up_to_id.
        Uses lexicographic ordering of base32hex(UUID v7) IDs."""
