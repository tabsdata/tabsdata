#
# Copyright 2024 Tabsdata Inc.
#

import fnmatch
import glob
import importlib
import importlib.util
import os
import shutil
import sys
import time
from types import ModuleType

EXAMPLES_GUIDES_BOOK_PATH = (
    "extensions/"
    "python/"
    "libraries/"
    "te_examples/"
    "src/"
    "main/"
    "python/"
    "tabsdata/"
    "extensions/"
    "_examples/"
    "guides/"
    "book"
)


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


def rmtree_with_retry(path, max_retries=16, delay=3):
    for attempt in range(max_retries):
        try:
            shutil.rmtree(path)
            if attempt > 0:
                logger.debug(f"✏️ Removed folder {path} after {attempt + 1} attempts")
            else:
                logger.debug(f"✏️ Successfully removed folder {path}")
            return
        except Exception as error:
            if attempt < max_retries - 1:
                logger.debug(
                    f"✏️ Attempt {attempt + 1}/{max_retries} failed "
                    f"removing folder {path}; "
                    f"retrying in {delay}s..."
                )
                time.sleep(delay)
            else:
                logger.debug(
                    f"✏️ Failed to remove folder {path} "
                    f"after {max_retries} attempts: {error}"
                )
                raise


def clean(project_folder, inclusion_patterns, exclusion_patterns):
    logger.debug(f"✏️ Cleaning project: {project_folder}")

    for pattern in inclusion_patterns:
        logger.debug(f"✏️ Processing pattern: '{pattern}'")
        for path in glob.glob(
            os.path.join(project_folder, "**", pattern), recursive=True
        ):
            full_path = os.path.join(project_folder, path)
            logger.debug(f"✏️ Found path: '{path}'")
            relative_path = os.path.relpath(full_path, project_folder)
            first_component = relative_path.split(os.sep, 1)[0]
            if (
                first_component.startswith(".")
                and first_component != ".pytest_cache"
                and first_component != ".coverage"
                and first_component != ".coveragex"
                and first_component != ".ruff_cache"
                and first_component != ".mypy_cache"
                and first_component != ".grimp_cache"
            ):
                logger.debug(f"✏️    - Skipping root dot file/folder: {full_path}")
                continue
            elif "target" in full_path and "deps" in full_path and pattern == "*.log.*":
                logger.debug(f"✏️    - Skipping removal of reserved file: {full_path}")
            elif os.path.isdir(full_path):
                rmtree_with_retry(full_path)
            elif os.path.isfile(full_path):
                basename = os.path.basename(path)
                if any(
                    fnmatch.fnmatch(basename, exclusion_pattern)
                    for exclusion_pattern in exclusion_patterns
                ):
                    logger.debug(f"✏️    - Skipping removal of file: {full_path}")
                else:
                    logger.debug(f"✏️    - Removing file: {full_path}")
                    os.remove(full_path)

    target_folder = os.path.join(project_folder, "target")
    if os.path.isdir(target_folder) and not os.listdir(target_folder):
        rmtree_with_retry(target_folder)


def clean_py(project_folder):
    def gather_connectors() -> list[str]:
        root = project_folder
        connectors_folders = [
            os.path.join("connectors", "python"),
            os.path.join("connectors.ee", "python"),
        ]

        tabsdata_connectors: list[str] = []

        for connectors_folder in connectors_folders:
            if not os.path.isdir(os.path.join(root, connectors_folder)):
                continue

            for entry in os.scandir(os.path.join(root, connectors_folder)):
                if entry.is_dir():
                    if not entry.name.startswith("tabsdata-"):
                        logger.debug(f"⛔️ Invalid connector folder name: {entry.name}")
                    else:
                        tabsdata_connectors.append(
                            os.path.join(connectors_folder, entry.name)
                        )
                        logger.debug(f"📦️ Inserting connector {entry}")
        return sorted(tabsdata_connectors)

    inclusion_patterns = [
        "__pycache__",
        ".benchmarks",
        ".cache",
        ".coverage*",
        ".egg-info",
        ".grimp_cache",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".tox",
        "*.egg-info",
        "*.log.*",
        "*.log",
        "client/platform/src/main/python/tabsdata/assets",
        "client/platform/src/main/python/tabsdata/resources",
        "client/platform/src/main/python/tabsdatak/assets",
        "client/platform/src/main/python/tabsdatak/resources",
        "coverage.xml",
        "docs/_build",
        "htmlcov",
        "local_dev",
        "local_development_artifacts",
        "site",
        "SOURCETRACK.yaml",
        "tabsdatak/ag/assets",
        "target/build",
        "target/pytest",
        "target/python",
        "target/reports",
        "target/tdlocal",
        "target/wheels",
        "tdlocal",
        EXAMPLES_GUIDES_BOOK_PATH,
    ]

    connectors = gather_connectors()
    for connector in connectors:
        for leaf in ("BANNER", "LICENSE"):
            inclusion_patterns.append(
                os.path.join(
                    connector,
                    "src",
                    "main",
                    "python",
                    "**",
                    "assets",
                    "manifest",
                    leaf,
                ),
            )
        for ext_suffix in ("*.so", "*.pyd"):
            inclusion_patterns.append(
                os.path.join(
                    "python",
                    "src",
                    "main",
                    "python",
                    "tabsdata",
                    "**",
                    ext_suffix,
                ),
            )

    exclusion_patterns = [
        ".coveragerc",
        "another_file.log",
        "data.log",
        "source_1.log",
        "source_2.log",
        "*.log.yaml",
        "*.log.py.yaml",
        "*.log.rs.yaml",
        "*.log.ts.yaml",
    ]

    clean(project_folder, inclusion_patterns, exclusion_patterns)


def clean_rs(project_folder):
    inclusion_patterns = [
        # "Cargo.lock",
        "*.log.*",
        "*.log",
        "books/dguide/book",
        "target",
        "variant/assets/manifest/THIRD-PARTY",
    ]

    exclusion_patterns = [
        ".coveragerc",
        "another_file.log",
        "data.log",
        "source_1.log",
        "source_2.log",
        "*.log.yaml",
        "*.log.py.yaml",
        "*.log.rs.yaml",
        "*.log.ts.yaml",
    ]

    clean(project_folder, inclusion_patterns, exclusion_patterns)


def clean_ts(project_folder):
    inclusion_patterns = [
        "node_modules",
        "src/e2e/playwright-report",
        "src/e2e/setup/storageState.json",
        "src/e2e/test-results",
        "src/tests/coverage",
        "src/tests/e2e/playwright-report",
        "src/tests/e2e/setup/storageState.json",
        "src/tests/e2e/test-results",
        "target",
        "test-results",
    ]

    exclusion_patterns = [
        ".coveragerc",
        "another_file.log",
        "data.log",
        "source_1.log",
        "source_2.log",
        "*.log.yaml",
        "*.log.py.yaml",
        "*.log.rs.yaml",
        "*.log.ts.yaml",
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
        elif target == "ts":
            clean_ts(project)
        else:
            logger.error(f"⭕️ Error: Unknown target '{target}'", file=sys.stderr)
            sys.exit(1)
    else:
        logger.error("⭕️ Error: No project or target provided", file=sys.stderr)
        sys.exit(1)
