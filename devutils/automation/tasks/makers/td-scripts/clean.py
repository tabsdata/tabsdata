#
# Copyright 2024 Tabs Data Inc.
#

import glob
import importlib
import importlib.util
import os
import shutil
import sys
import time
from types import ModuleType

EXAMPLES_GUIDES_BOOK_PATH = (
    "extensions/python/td-lib/te_examples/tabsdata/extensions/_examples/guides/book"
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
            ):
                logger.debug(f"✏️    - Skipping root dot file/folder: {full_path}")
                continue
            elif "target" in full_path and "deps" in full_path and pattern == "*.log.*":
                logger.debug(f"✏️    - Skipping removal of reserved file: {full_path}")
            elif os.path.isdir(full_path):
                rmtree_with_retry(full_path)
            elif os.path.isfile(full_path):
                basename = os.path.basename(path)
                if basename in exclusion_patterns:
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
            os.path.join(root, "connectors", "python"),
            # os.path.join(root, "connectors.ee", "python"),
        ]

        tabsdata_connectors: list[str] = []

        for connectors_folder in connectors_folders:
            if not os.path.isdir(connectors_folder):
                continue

            for entry in os.scandir(connectors_folder):
                if entry.is_dir():
                    connector_name_parts = entry.name.split("-", 1)
                    if (
                        len(connector_name_parts) != 2
                        or connector_name_parts[0] != "tabsdata"
                    ):
                        logger.debug(f"⛔️ Invalid connector folder name: {entry.name}")
                    else:
                        tabsdata_connectors.append(entry.name)
                        logger.debug(f"📦️ Inserting connector {entry}")
        return sorted(tabsdata_connectors)

    inclusion_patterns = [
        "__pycache__",
        ".benchmarks",
        ".cache",
        ".coverage*",
        ".egg-info",
        ".mypy_cache",
        ".pytest_cache",
        ".tox",
        "*.egg-info",
        "PYDOC.csv",
        "target/build",
        "target/pydoc",
        "target/pytest",
        "target/python",
        "target/reports",
        "target/tdlocal",
        "target/wheels",
        "coverage.xml",
        "docs/_build",
        "htmlcov",
        "*.log",
        "*.log.*",
        "site",
        "local_dev",
        "tdlocal",
        "local_development_artifacts",
        "client/td-sdk/tabsdata/assets",
        "client/td-sdk/tabsdata/resources",
        "tabsdata_agent/assets",
        "SOURCETRACK.yaml",
        "tabsdata.libs",
        EXAMPLES_GUIDES_BOOK_PATH,
    ]

    connectors = gather_connectors()
    for connector in connectors:
        connector_module = connector.replace("-", "_")
        inclusion_patterns.append(
            os.path.join(
                "connectors",
                "python",
                connector,
                connector_module,
                "assets",
                "manifest",
                "BANNER",
            ),
        )
        inclusion_patterns.append(
            os.path.join(
                "connectors",
                "python",
                connector,
                connector_module,
                "assets",
                "manifest",
                "LICENSE",
            ),
        )

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
        # "Cargo.lock",
        "target",
        "*.log",
        "*.log.*",
        "books/dguide/book",
        "variant/assets/manifest/THIRD-PARTY",
    ]

    exclusion_patterns = [
        ".coveragerc",
        "data.log",
        "another_file.log",
        "source_1.log",
        "source_2.log",
    ]

    clean(project_folder, inclusion_patterns, exclusion_patterns)


def clean_ts(project_folder):
    inclusion_patterns = [
        "node_modules",
        "src/tests/coverage",
        "src/e2e/test-results",
        "src/e2e/playwright-report",
        "target",
        "test-results",
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
        elif target == "ts":
            clean_ts(project)
        else:
            logger.error(f"⭕️ Error: Unknown target '{target}'", file=sys.stderr)
            sys.exit(1)
    else:
        logger.error("⭕️ Error: No project or target provided", file=sys.stderr)
        sys.exit(1)
