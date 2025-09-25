#
# Copyright 2025 Tabs Data Inc.
#

import argparse
import importlib
import importlib.util
import os
import subprocess
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

TABSDATA_EE = "tabsdata-ee"
TABSDATA_AG = "tabsdata-ag"
TABSDATA_UI = "tabsdata-ui"

IGNORED_FOLDERS = {
    ".git",
    ".idea",
    ".ipynb_checkpoints",
    "node_modules",
    ".pytest_cache",
    ".tabsdata",
    "__pycache__",
    "target",
}

IGNORED_EXTENSIONS = {
    ".coverage",
    ".DS_Store",
    ".lock",
    ".parquet",
    "THIRD-PARTY",
}


def get_old_version(root_folder) -> str:
    version_file = os.path.join(
        root_folder,
        "..",
        "tabsdata-os",
        "assets",
        "manifest",
        "VERSION",
    )
    if not os.path.exists(version_file):
        logger.error(f"❌ Error: VERSION file not found at {version_file}")
        exit(1)
    with open(version_file, "r", encoding="utf-8") as file:
        version = file.read().strip()
    if not version:
        logger.error("❌ Error: VERSION file is empty")
        exit(1)
    return version


def expand_single_level_wildcards(root_folder: str, pattern: str) -> set[str]:
    parts = pattern.split("/")
    paths = [""]  # relative to root_folder

    for part in parts:
        new_paths = []
        for relative_base in paths:
            absolute_base = (
                os.path.join(root_folder, relative_base)
                if relative_base
                else root_folder
            )
            if part == "*":
                try:
                    entries = os.listdir(absolute_base)
                except FileNotFoundError:
                    continue
                for entry in entries:
                    entry_relative_path = (
                        f"{relative_base}/{entry}" if relative_base else entry
                    )
                    entry_absolute_path = os.path.join(root_folder, entry_relative_path)
                    if os.path.isdir(entry_absolute_path):
                        new_paths.append(entry_relative_path)
            else:
                next_relative_path = (
                    f"{relative_base}/{part}" if relative_base else part
                )
                new_paths.append(next_relative_path)
        paths = new_paths

    result = set()
    for relative_path in paths:
        absolute_path = os.path.join(root_folder, relative_path)
        if os.path.isfile(absolute_path):
            result.add(os.path.abspath(absolute_path))
    return result


def get_bump_files(root_folder) -> list[str]:
    bump_files_file = os.path.join(
        root_folder,
        ".custom",
        "bump.cfg",
    )
    logger.info(f"🔖 Using bump.cfg file {os.path.realpath(bump_files_file)}")
    if not os.path.exists(bump_files_file):
        logger.error(f"❌ Error: bump.cfg file not found at {bump_files_file}")
        exit(1)
    bump_files = set()
    with open(bump_files_file, "r", encoding="utf-8") as file:
        for line in file:
            pattern = line.strip()
            if not pattern or pattern.startswith("#"):
                continue
            matches = expand_single_level_wildcards(root_folder, pattern)
            if not matches:
                logger.warning(f"⚠️ No match for bump pattern: {pattern}")
            else:
                bump_files.update(matches)

    if not bump_files:
        logger.warning("⚠️ Warning: bump.cfg is empty or has no matching files")

    return sorted(bump_files)


def bump_version_in_file(path, old_version, new_version, bump_files, warnings):
    if path in bump_files:
        try:
            with open(path, "r", encoding="utf-8") as file_r:
                content = file_r.read()
                if old_version in content:
                    content = content.replace(old_version, new_version)
                    with open(path, "w", encoding="utf-8") as file_w:
                        file_w.write(content)
                    logger.debug(f"✅ Bumped version in file {path}")
                else:
                    logger.debug(f"🌀 Skipping bumping version in file {path}")
        except (UnicodeDecodeError, PermissionError) as e:
            logger.error(f"❌ Error reading required file {path}: {e}")
    else:
        if any(path.endswith(extension) for extension in IGNORED_EXTENSIONS):
            return
        try:
            with open(path, "r", encoding="utf-8") as file:
                content = file.read()
                if old_version in content:
                    warnings.append(path)
                else:
                    pass
        except (UnicodeDecodeError, PermissionError):
            pass


def bump_version(root_folder, old_version, new_version, bump_files):
    root_folder = os.path.abspath(root_folder)
    warnings = []
    for folder, folders, files in os.walk(root_folder):
        folders[:] = [d for d in folders if d not in IGNORED_FOLDERS]
        for file in files:
            path = os.path.join(folder, file)
            bump_version_in_file(path, old_version, new_version, bump_files, warnings)
    for warning in warnings:
        logger.warning(
            f"‼️ File {warning} contains the old version but is not in the bump list"
        )


def bump(root: str, old_version: str, new_version: str):
    logger.info(f"✏️ Upgrading root {root}")

    bump_files = get_bump_files(root)

    logger.debug(
        f"🔍 Replacing '{old_version}' with '{new_version}' in defined files:\n"
        + "\n".join(f"   - {file}" for file in bump_files)
    )

    bump_version(root, old_version, new_version, bump_files)


def main():
    parser = argparse.ArgumentParser(description="Bump version of non-cargo files")
    parser.add_argument("root", type=str, help="Root folder")
    parser.add_argument("version", type=str, help="New version")
    args = parser.parse_args()

    old_version = get_old_version(args.root)
    new_version = args.version

    bump(args.root, old_version, new_version)
    root = os.path.basename(args.root.rstrip(os.sep))
    parent = Path(args.root).resolve().parent
    if root == TABSDATA_UI:
        subprocess.run(
            [
                "npm",
                "version",
                new_version,
                "--no-git-tag-version",
            ],
            cwd=os.path.join(parent, TABSDATA_UI),
            capture_output=False,
            text=True,
            encoding="utf-8",
            errors="strict",
        )
    else:
        logger.info(f"✂️ No need for additional upgrades for root: {root}")


if __name__ == "__main__":
    main()
