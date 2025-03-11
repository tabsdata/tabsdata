#
# Copyright 2024 Tabs Data Inc.
#

import glob
import importlib
import importlib.util
import os
import shutil
import sys
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


def clean(project_folder, inclusion_patterns, exclusion_patterns):
    logger.debug(f"✏️ Cleaning project: {project_folder}")

    for pattern in inclusion_patterns:
        logger.debug(f"✏️ Processing pattern: '{pattern}'")
        for path in glob.glob(
            os.path.join(project_folder, "**", pattern), recursive=True
        ):
            full_path = os.path.join(project_folder, path)
            logger.debug(f"✏️ Found path: '{path}'")
            if os.path.isdir(full_path):
                logger.debug(f"✏️    - Removing directory: {full_path}")
                shutil.rmtree(full_path)
            elif os.path.isfile(full_path):
                basename = os.path.basename(path)
                if basename in exclusion_patterns:
                    logger.debug(f"✏️    - Skipping removal of file: {full_path}")
                else:
                    logger.debug(f"✏️    - Removing file: {full_path}")
                    os.remove(full_path)


def clean_py(project_folder):
    inclusion_patterns = [
        "__pycache__",
        ".cache",
        ".coverage*",
        ".egg-info",
        ".mypy_cache",
        ".pytest_cache",
        ".tox",
        "*.egg-info",
        "build",
        "coverage.xml",
        "dist",
        "docs/_build",
        "htmlcov",
        "*.log",
        "*.log.*",
        "site",
        "local_dev",
        "local_development_artifacts",
        "variant/assets/manifest/THIRD-PARTY",
        "client/td-sdk/tabsdata/assets",
    ]

    exclusion_patterns = [
        ".coveragerc",
        "data.log",
        "another_file.log",
        "source_1.log",
        "source_2.log",
    ]

    clean(project_folder, inclusion_patterns, exclusion_patterns)


def clean_rs(project_folder):
    inclusion_patterns = [
        "Cargo.lock",
        "target",
        "*.log",
        "*.log.*",
    ]

    exclusion_patterns = [
        ".coveragerc",
        "data.log",
        "another_file.log",
        "source_1.log",
        "source_2.log",
    ]

    clean(project_folder, inclusion_patterns, exclusion_patterns)


if __name__ == "__main__":
    if len(sys.argv) > 2:
        project = sys.argv[1]
        target = sys.argv[2]
        if target == "py":
            clean_py(project)
        elif target == "rs":
            clean_rs(project)
        else:
            logger.error(f"⭕️ Error: Unknown target '{target}'", file=sys.stderr)
            sys.exit(1)
    else:
        logger.error("⭕️ Error: No project or target provided", file=sys.stderr)
        sys.exit(1)
