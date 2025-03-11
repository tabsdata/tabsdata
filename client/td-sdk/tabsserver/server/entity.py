#
# Copyright 2025 Tabs Data Inc.
#

from abc import ABC, abstractmethod
from pathlib import Path

from packaging.version import Version


class Upgrade(ABC):
    source_version: Version
    target_version: Version

    @abstractmethod
    def upgrade(
        self,
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        pass
