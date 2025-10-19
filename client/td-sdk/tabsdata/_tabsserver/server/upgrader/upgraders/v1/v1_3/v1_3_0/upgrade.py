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
class Upgrade_1_2_0_to_1_3_0(Upgrade):
    source_version = Version("1.2.0")
    target_version = Version("1.3.0")

    def upgrade(
        self,
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        return []
