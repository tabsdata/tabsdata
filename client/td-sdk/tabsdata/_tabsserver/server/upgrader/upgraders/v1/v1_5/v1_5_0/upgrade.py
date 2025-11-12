#
# Copyright 2025 Tabs Data Inc.
#

import logging
import shutil
from pathlib import Path

from packaging.version import Version

from tabsdata._tabsserver.server.upgrader.entity import Upgrade
from tabsdata._utils.internal._resources import td_resource

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# noinspection PyPep8Naming
class Upgrade_1_4_0_to_1_5_0(Upgrade):
    source_version = Version("1.4.0")
    target_version = Version("1.5.0")

    # noinspection DuplicatedCode
    def upgrade(
        self,
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        messages: list[str] = []

        messages.extend(self.upgrade_instance(instance, dry_run))

        return messages

    @staticmethod
    def upgrade_instance(  # noqa: C901
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        messages: list[str] = []

        target_logging_yaml_file = instance / "workspace" / "config" / "logging.yaml"
        if dry_run:
            messages.append(f"[dry-run] Will create file '{target_logging_yaml_file}'.")
            _ = td_resource("resources/profile/workspace/config/logging.yaml")
        else:
            logger.debug(
                f"Provisioning configuration file '{target_logging_yaml_file}'."
            )
            # noinspection PyBroadException
            try:
                source_logging_yaml_file = td_resource(
                    "resources/profile/workspace/config/logging.yaml"
                )
                shutil.copy(
                    source_logging_yaml_file,
                    target_logging_yaml_file,
                    follow_symlinks=True,
                )
                messages.append(f"Created file '{target_logging_yaml_file}'.")
            except Exception:
                logger.debug(
                    "Expected logging.yaml config file is missing; "
                    "skipping folder provisioning."
                )
        return messages


if __name__ == "__main__":
    print(
        Upgrade_1_4_0_to_1_5_0().upgrade(
            Path("~/.tabsdata/instances/tabsdata").expanduser(),
            dry_run=False,
        )
    )
