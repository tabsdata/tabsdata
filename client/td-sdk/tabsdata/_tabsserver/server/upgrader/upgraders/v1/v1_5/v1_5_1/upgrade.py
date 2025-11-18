#
# Copyright 2025 Tabs Data Inc.
#

import logging
from pathlib import Path

from packaging.version import Version

from tabsdata._tabsserver.server.upgrader.entity import Upgrade
from tabsdata._utils.constants import TABSDATA_MODULE_NAME

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# noinspection PyPep8Naming,DuplicatedCode
class Upgrade_1_5_0_to_1_5_1(Upgrade):
    source_version = Version("1.5.0")
    target_version = Version("1.5.1")

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
        Upgrade_1_5_0_to_1_5_1().upgrade(
            Path("~/.tabsdata/instances/tabsdata").expanduser(),
            dry_run=False,
        )
    )
