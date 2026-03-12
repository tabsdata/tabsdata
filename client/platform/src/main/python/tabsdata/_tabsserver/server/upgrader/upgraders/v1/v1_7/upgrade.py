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
class Upgrade_1_6_to_1_7(Upgrade):
    source_version = Version("1.6")
    target_version = Version("1.7")

    def upgrade(
        self,
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        return []


if __name__ == "__main__":
    print(
        Upgrade_1_6_to_1_7().upgrade(
            Path("~/.tabsdata/instances/tabsdata").expanduser(),
            dry_run=True,
        )
    )
