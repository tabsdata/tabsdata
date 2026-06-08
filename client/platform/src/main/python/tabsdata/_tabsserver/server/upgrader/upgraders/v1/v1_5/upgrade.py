#
# Copyright 2025 Tabs Data Inc.
#

import logging
import shutil
from pathlib import Path

from packaging.version import Version

from tabsdata._tabsserver.server.upgrader.entity import Upgrade
from tabsdata._utils.constants import TABSDATA_MODULE_NAME
from tabsdatak.common._resources.reader import td_resource

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# noinspection PyPep8Naming
class Upgrade_1_4_to_1_5(Upgrade):
    source_version = Version("1.4")
    target_version = Version("1.5")

    # noinspection DuplicatedCode
    def upgrade(
        self,
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        messages: list[str] = []

        messages.extend(self.upgrade_instance_logging_yaml(instance, dry_run))
        messages.extend(self.upgrade_instance_requirements(instance, dry_run))

        return messages

    @staticmethod
    def upgrade_instance_logging_yaml(  # noqa: C901
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

    @staticmethod
    def upgrade_instance_requirements(  # noqa: C901
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        messages: list[str] = []

        config_folder = instance / "workspace" / "config"
        for requirements_file in config_folder.rglob("requirements.txt"):
            lines = requirements_file.read_text().splitlines()
            modified_lines = [
                line
                for line in lines
                if line.startswith(TABSDATA_MODULE_NAME) and "==" in line
            ]
            if not modified_lines:
                continue
            if dry_run:
                messages.append(
                    "[dry-run] Will strip versions from tabsdata requirements "
                    f"in '{requirements_file}': {modified_lines}."
                )
            else:
                logger.debug(
                    "Stripping versions from tabsdata requirements "
                    f"in '{requirements_file}'."
                )
                # noinspection PyBroadException
                try:
                    updated_lines = []
                    for line in lines:
                        if line.startswith(TABSDATA_MODULE_NAME):
                            line = line.split("==")[0]
                        updated_lines.append(line)
                    requirements_file.write_text("\n".join(updated_lines))
                    messages.append(
                        "Stripped versions from tabsdata requirements "
                        f"in '{requirements_file}': {modified_lines}."
                    )
                except Exception:
                    logger.debug(
                        f"Error processing requirements file '{requirements_file}'; "
                        "skipping."
                    )
        return messages


if __name__ == "__main__":
    print(
        Upgrade_1_4_to_1_5().upgrade(
            Path("~/.tabsdata/instances/tabsdata").expanduser(),
            dry_run=False,
        )
    )
