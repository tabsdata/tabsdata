#
# Copyright 2025 Tabs Data Inc.
#

import logging
from pathlib import Path

from packaging.version import Version

from tabsdata.tabsserver.server.entity import Upgrade

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# noinspection PyPep8Naming
class Upgrade_0_9_1_to_0_9_2(Upgrade):
    source_version = Version("0.9.1")
    target_version = Version("0.9.2")

    def upgrade(
        self,
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        return []
