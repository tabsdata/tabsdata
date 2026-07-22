#
# Copyright 2025 Tabsdata Inc.
#

import argparse
import importlib
import importlib.util
import os
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
}

PROJECTS = [
    "tabsdata",
    "kminus",
    "tabsdata-ee",
    "tabsdata-os",
    "tabsdata-km",
    "tabsdata-ag",
    "tabsdata-ui",
]


def scan_project(project_path: str, old_version: str) -> list[str]:
    matches = []
    for folder, folders, files in os.walk(project_path, followlinks=False):
        folders[:] = [
            directory
            for directory in folders
            if directory not in IGNORED_FOLDERS
            and not os.path.islink(os.path.join(folder, directory))
        ]
        for file in files:
            path = os.path.join(folder, file)
            if os.path.islink(path):
                continue
            if any(path.endswith(ext) for ext in IGNORED_EXTENSIONS):
                continue
            try:
                with open(path, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                    matching_lines = [
                        (i + 1, line.rstrip("\n"))
                        for i, line in enumerate(lines)
                        if old_version in line
                    ]
                    if matching_lines:
                        matches.append((
                            os.path.relpath(path, project_path),
                            matching_lines,
                        ))
            except (UnicodeDecodeError, PermissionError, OSError):
                pass
    return sorted(matches, key=lambda m: m[0])


def main():
    parser = argparse.ArgumentParser(
        description="Scan projects for remaining old version references",
    )
    parser.add_argument(
        "base_folder",
        type=str,
        help="Base folder containing projects",
    )
    parser.add_argument(
        "old_version",
        type=str,
        help="Old version to search for",
    )
    parser.add_argument(
        "new_version",
        type=str,
        help="New version (for display)",
    )
    args = parser.parse_args()

    base = os.path.abspath(args.base_folder)
    old_version = args.old_version
    new_version = args.new_version

    found_any = False

    print()
    print(f"\033[1;36m{'=' * 72}\033[0m")
    print(
        f"\033[1;36m  Scanning for remaining references "
        f"to old version \033[1;33m{old_version}\033[0m"
    )
    print(f"\033[1;36m  Expected version: \033[1;32m{new_version}\033[0m")
    print(f"\033[1;36m{'=' * 72}\033[0m")

    for project_name in PROJECTS:
        project_path = os.path.join(base, project_name)
        if not os.path.isdir(project_path):
            continue

        matches = scan_project(project_path, old_version)

        print()
        if matches:
            found_any = True
            print(
                f"\033[1;35m  {project_name}\033[0m  "
                f"\033[1;38;5;208m("
                f"{len(matches)} file{'s' if len(matches) != 1 else ''}"
                f")\033[0m"
            )
            print(f"\033[38;5;243m  {'─' * 60}\033[0m")
            for path, lines in matches:
                print(f"\033[1;38;5;208m    ⚠  {path}\033[0m")
                for line_no, line_text in lines:
                    print(f"\033[38;5;214m       {line_no}: {line_text}\033[0m")
        else:
            print(f"\033[1;35m  {project_name}\033[0m  \033[1;32m✓ clean\033[0m")

    print()
    if found_any:
        print(
            f"\033[1;38;5;208m  ⚠  Some files still reference "
            f"version {old_version}\033[0m"
        )
    else:
        print(
            f"\033[1;32m  ✓  All projects are clean — "
            f"no references to {old_version} found\033[0m"
        )
    print(f"\033[1;36m{'=' * 72}\033[0m")
    print()


if __name__ == "__main__":
    main()
