#
# Copyright 2025 Tabsdata Inc.
#

import logging
from pathlib import Path

from packaging.version import Version

from tabsdata._tabsserver.server.upgrader.entity import Upgrade

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# noinspection PyPep8Naming
class Upgrade_0_9_to_1_0(Upgrade):
    source_version = Version("0.9")
    target_version = Version("1.0")

    def upgrade(
        self,
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        return []


if __name__ == "__main__":
    print(
        Upgrade_0_9_to_1_0().upgrade(
            Path("~/.tabsdata/instances/tabsdata").expanduser(),
            dry_run=True,
        )
    )
