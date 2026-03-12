#
# Copyright 2026 Tabs Data Inc.
#

import logging
from pathlib import Path

from packaging.version import Version

from tabsdata._tabsserver.server.upgrader.entity import Upgrade

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# noinspection PyPep8Naming
class Upgrade_1_7_0_to_1_7_1(Upgrade):
    source_version = Version("1.7.0")
    target_version = Version("1.7.1")

    def upgrade(
        self,
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        return []
