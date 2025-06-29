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
            if filename.endswith(".whl"):
                yield Path(dirpath) / filename


def main(profile: str):
    source_folder = Path(".").resolve()
    target_folder = Path("./target/wheels").resolve()
    target_folder.mkdir(parents=True, exist_ok=True)
    target_archive = os.path.join(target_folder, f"tabsdata-{profile}.tar.gz")
    with tarfile.open(target_archive, "w:gz") as tar:
        for wheel in wheels(source_folder):
            logger.info(f"Adding wheel: {wheel}")
            tar.add(wheel, arcname=wheel.name)
    logger.info(f"Created file {target_archive} with wheels from {source_folder}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        profile = sys.argv[1]
    else:
        logger.error("⭕️ Error: No profile provided", file=sys.stderr)
        sys.exit(1)
    main(profile)
