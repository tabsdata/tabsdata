#
#  Copyright 2025 Tabs Data Inc.
#

#
#

import argparse
import logging
import os.path
from pathlib import Path
from typing import Dict, Type

import humanize
from packaging.version import Version

from tabsdata._tabsserver.server.instance import (
    VERSION_FILE,
    get_instance_path,
    get_version,
)
from tabsdata._tabsserver.server.upgrader.entity import Upgrade
from tabsdata._tabsserver.server.upgrader.upgraders.v0.v0_9.v0_9_1.upgrade import (
    Upgrade_0_9_0_to_0_9_1,
)
from tabsdata._tabsserver.server.upgrader.upgraders.v0.v0_9.v0_9_2.upgrade import (
    Upgrade_0_9_1_to_0_9_2,
)
from tabsdata._tabsserver.server.upgrader.upgraders.v0.v0_9.v0_9_3.upgrade import (
    Upgrade_0_9_2_to_0_9_3,
)
from tabsdata._tabsserver.server.upgrader.upgraders.v0.v0_9.v0_9_4.upgrade import (
    Upgrade_0_9_3_to_0_9_4,
)
from tabsdata._tabsserver.server.upgrader.upgraders.v0.v0_9.v0_9_5.upgrade import (
    Upgrade_0_9_4_to_0_9_5,
)
from tabsdata._tabsserver.server.upgrader.upgraders.v0.v0_9.v0_9_6.upgrade import (
    Upgrade_0_9_5_to_0_9_6,
)
from tabsdata._tabsserver.server.upgrader.upgraders.v1.v1_0.v1_0_0.upgrade import (
    Upgrade_0_9_6_to_1_0_0,
)
from tabsdata._tabsserver.server.upgrader.upgraders.v1.v1_1.v1_1_0.upgrade import (
    Upgrade_1_0_0_to_1_1_0,
)
from tabsdata._tabsserver.server.upgrader.upgraders.v1.v1_2.v1_2_0.upgrade import (
    Upgrade_1_1_0_to_1_2_0,
)
from tabsdata._tabsserver.server.upgrader.upgraders.v1.v1_3.v1_3_0.upgrade import (
    Upgrade_1_2_0_to_1_3_0,
)
from tabsdata._tabsserver.server.upgrader.upgraders.v1.v1_4.v1_4_0.upgrade import (
    Upgrade_1_3_0_to_1_4_0,
)
from tabsdata._tabsserver.server.upgrader.upgraders.v1.v1_5.v1_5_0.upgrade import (
    Upgrade_1_4_0_to_1_5_0,
)
from tabsdata._tabsserver.utils import TimeBlock

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


SEED_VERSION = Version("0.9.0")

UPGRADE_PLAN: Dict[Version, Type[Upgrade]] = {
    Upgrade_0_9_0_to_0_9_1.source_version: Upgrade_0_9_0_to_0_9_1,
    Upgrade_0_9_1_to_0_9_2.source_version: Upgrade_0_9_1_to_0_9_2,
    Upgrade_0_9_2_to_0_9_3.source_version: Upgrade_0_9_2_to_0_9_3,
    Upgrade_0_9_3_to_0_9_4.source_version: Upgrade_0_9_3_to_0_9_4,
    Upgrade_0_9_4_to_0_9_5.source_version: Upgrade_0_9_4_to_0_9_5,
    Upgrade_0_9_5_to_0_9_6.source_version: Upgrade_0_9_5_to_0_9_6,
    Upgrade_0_9_6_to_1_0_0.source_version: Upgrade_0_9_6_to_1_0_0,
    Upgrade_1_0_0_to_1_1_0.source_version: Upgrade_1_0_0_to_1_1_0,
    Upgrade_1_1_0_to_1_2_0.source_version: Upgrade_1_1_0_to_1_2_0,
    Upgrade_1_2_0_to_1_3_0.source_version: Upgrade_1_2_0_to_1_3_0,
    Upgrade_1_3_0_to_1_4_0.source_version: Upgrade_1_3_0_to_1_4_0,
    Upgrade_1_4_0_to_1_5_0.source_version: Upgrade_1_4_0_to_1_5_0,
}


def get_upgrade_plan() -> Dict[Version, Type[Upgrade]]:
    return UPGRADE_PLAN


def get_source_version(instance: Path) -> Version:
    version_file = os.path.join(instance, VERSION_FILE)
    if os.path.exists(version_file):
        with open(version_file, "r", encoding="utf-8") as file:
            version = file.read().strip()
        if not version:
            print("The file .version for this instance is empty")
            exit(1)
    else:
        return SEED_VERSION
    return Version(version)


def get_target_version() -> Version:
    return get_version()


def update_version(instance: Path, version: Version):
    version_file = instance / VERSION_FILE
    try:
        with open(version_file, "w", encoding="utf-8") as file:
            file.write(str(version))
    except OSError as e:
        raise RuntimeError(f"Error writing to .version file: {e}")


def check(upgrade_plan: Dict[Version, Type[Upgrade]]):
    visited_versions = set()
    current_version = SEED_VERSION
    while current_version in upgrade_plan:
        if current_version in visited_versions:
            raise RuntimeError(f"Loop detected in upgrade plan: {current_version}")
        visited_versions.add(current_version)
        upgrade_class = upgrade_plan[current_version]
        upgrade_object = upgrade_class()
        current_version = upgrade_object.target_version
    all_versions = set(upgrade_plan.keys())
    missed_versions = all_versions - visited_versions
    if missed_versions:
        raise RuntimeError(f"Some versions cannot be reached: {missed_versions}")


def upgrade(
    instance: Path,
    source_version: Version,
    target_version: Version,
    upgrade_plan: Dict[Version, Type[Upgrade]],
    dry_run: bool,
) -> Dict[Version, list[str]]:
    check(upgrade_plan)
    actions: Dict[Version, list[str]] = {}
    if source_version >= target_version:
        return actions
    if dry_run:
        logger.info(
            f"Simulating upgrade of instance '{instance}' "
            f"from version {source_version} "
            f"to version {target_version}"
        )
    else:
        logger.info(
            f"Upgrading instance '{instance}' "
            f"from version {source_version} "
            f"to version {target_version}"
        )
    current_version = source_version
    while current_version < target_version:
        if current_version not in upgrade_plan:
            raise RuntimeError(f"No upgrade class found for {source_version}")
        upgrade_class = upgrade_plan[current_version]
        upgrade_object = upgrade_class()
        timer = TimeBlock()
        with timer:
            if dry_run:
                logger.info(
                    f"Simulating upgrade (step) of instance {instance} "
                    f"from {upgrade_object.source_version} "
                    f"to {upgrade_object.target_version}..."
                )
            else:
                logger.info(
                    f"Upgrading (step) instance {instance} "
                    f"from {upgrade_object.source_version} "
                    f"to {upgrade_object.target_version}..."
                )
            actions[upgrade_object.target_version] = upgrade_object.upgrade(
                instance, dry_run
            )
        time_taken = timer.time_taken()
        logger.info(
            "Time taken:"
            f"{humanize.precisedelta(time_taken, minimum_unit='milliseconds')}"
        )
        current_version = upgrade_object.target_version

    if not dry_run:
        update_version(instance, target_version)

    return actions


def main():
    parser = argparse.ArgumentParser(
        description="Upgrade a Tabsdata instance to the current version"
    )
    parser.add_argument(
        "--instance",
        type=str,
        help="Path of the Tabsdata instance to upgrade",
        required=False,
    )
    parser.add_argument(
        "--execute",
        action="store_false",
        dest="dry_run",
        help="Disable dry-run mode and execute actions",
    )
    arguments = parser.parse_args()
    instance = get_instance_path(arguments.instance)
    source_version = get_source_version(instance)
    target_version = get_target_version()

    timer = TimeBlock()
    with timer:
        upgrades = upgrade(
            instance,
            source_version,
            target_version,
            get_upgrade_plan(),
            arguments.dry_run,
        )
    time_taken = timer.time_taken()

    for version, actions in sorted(upgrades.items()):
        logger.info(f"- Actions to version {version}:")
        if actions:
            for action in actions:
                logger.info(f"  - {action}")
        else:
            logger.info("  - No actions")

    if upgrades:
        logger.info(
            "Time taken to upgrade:"
            f"{humanize.precisedelta(time_taken, minimum_unit='milliseconds')}"
        )


if __name__ == "__main__":
    main()
