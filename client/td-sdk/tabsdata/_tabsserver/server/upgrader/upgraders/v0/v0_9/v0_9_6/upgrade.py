#
# Copyright 2025 Tabs Data Inc.
#

import logging
from pathlib import Path

from packaging.version import Version

from tabsdata._tabsserver.server.upgrader.entity import Upgrade

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# noinspection PyPep8Naming
class Upgrade_0_9_5_to_0_9_6(Upgrade):
    source_version = Version("0.9.5")
    target_version = Version("0.9.6")

    def upgrade(
        self,
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        return []
