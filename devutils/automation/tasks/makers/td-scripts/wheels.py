    #
# Copyright 2025 Tabs Data Inc.
#

import importlib
import importlib.util
import os
import os.path
import sys
import tarfile
from pathlib import Path
from types import ModuleType


# noinspection DuplicatedCode
def load(module_name) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        module_name,
        os.path.join(
            os.getenv("MAKE_LIBRARIES_PATH"),
            f"{module_name}.py",
        ),
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


logger = load("log").get_logger()


def wheels(root: Path):
    for dirpath, dirnames, filenames in os.walk(root, followlinks=True):
        dirnames.sort()
        filenames.sort()
        for filename in filenames:
            if filename.startswith("tabsdata") and filename.endswith(".whl"):
                yield Path(dirpath) / filename


def main(wheel_profile: str, wheel_flavour: str):
    tabsdata_source_folder = Path(".").resolve()
    tabsdata_ag_source_folder = Path("../tabsdata-ag").resolve()
    target_folder = Path("./target/wheels").resolve()
    target_folder.mkdir(parents=True, exist_ok=True)
    target_archive = os.path.join(
        target_folder, f"tabsdata-{wheel_profile}-{wheel_flavour}.tar.gz"
    )
    with tarfile.open(target_archive, "w:gz") as tar:
        for wheel in wheels(tabsdata_source_folder):
            logger.info(
                f"🧲 Adding wheel from tabsdata: 🎡 {wheel} to archive 📦"
                f" {target_archive}"
            )
            tar.add(wheel, arcname=wheel.name)
        if (
            tabsdata_ag_source_folder.exists()
            and tabsdata_ag_source_folder != tabsdata_source_folder
        ):
            for wheel in wheels(tabsdata_ag_source_folder):
                logger.info(
                    f"🧲 Adding wheel from tabsdata agent: 🎡 {wheel} to archive 📦"
                    f" {target_archive}"
                )
                tar.add(wheel, arcname=wheel.name)
    logger.info(
        f"Created file {target_archive} with wheels from {tabsdata_source_folder}"
    )


if __name__ == "__main__":
    if len(sys.argv) > 1:
        profile = sys.argv[1]
    else:
        logger.error("⭕️ Error: No profile provided", file=sys.stderr)
        sys.exit(1)

    if len(sys.argv) > 2:
        flavour = sys.argv[2]
    else:
        logger.error("⭕️ Error: No flavour provided", file=sys.stderr)
        sys.exit(1)

    main(profile, flavour)
