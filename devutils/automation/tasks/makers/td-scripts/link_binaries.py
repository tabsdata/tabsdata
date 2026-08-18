#
# Copyright 2026 Tabsdata Inc.
#

import argparse
import os
import sys
import tomllib
from pathlib import Path

SETUP_TOML_FILE = "setup.toml"
KEY_BINARY_GROUPS = "binary_groups"
KEY_NAMES = "names"
KEY_INSTALL_DEST = "install_dest"
KEY_KM_SUBPATH = "km_subpath"
KEY_SOURCE = "source"

KM_PROJECT_NAMES = ("tabsdata-km", "kminus")


def setup_toml_files(root: Path) -> list[Path]:
    files = []
    root_setup_toml = root / SETUP_TOML_FILE
    if root_setup_toml.is_file():
        files.append(root_setup_toml)
    connectors = root / "connectors" / "python"
    if connectors.is_dir():
        files.extend(sorted(connectors.glob(f"*/{SETUP_TOML_FILE}")))
    return files


def resolve_km_root(root: Path) -> Path | None:
    for name in KM_PROJECT_NAMES:
        candidate = root.parent / name
        if candidate.is_dir():
            return candidate
    return None


def source_folder(root: Path, group: dict, target_folder: Path) -> Path | None:
    km_subpath = group.get(KEY_KM_SUBPATH)
    if km_subpath is not None:
        km_root = resolve_km_root(root)
        return None if km_root is None else km_root / km_subpath
    source = group.get(KEY_SOURCE)
    if source is not None:
        return root / source
    return target_folder


def executable(name: str) -> str:
    return f"{name}.exe" if os.name == "nt" else name


def link(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() or target.is_symlink():
        target.unlink()
    try:
        target.symlink_to(source)
    except OSError:
        import shutil

        shutil.copy2(source, target)


def unlink(target: Path) -> None:
    if target.exists() or target.is_symlink():
        target.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Link or unlink the tabsdata binaries of every setup.toml"
    )
    parser.add_argument("root", type=Path, help="Project root folder")
    parser.add_argument("scripts", type=Path, help="Python scripts folder")
    parser.add_argument("target", type=Path, help="Cargo target release folder")
    parser.add_argument(
        "--unlink",
        action="store_true",
        help="Remove the links instead of creating them",
    )
    arguments = parser.parse_args()

    root = arguments.root.resolve()
    scripts = arguments.scripts.resolve()
    prefix = scripts.parent
    scripts_destination = scripts.name

    linked = 0
    for setup_toml in setup_toml_files(root):
        groups = tomllib.loads(setup_toml.read_text(encoding="utf-8")).get(
            KEY_BINARY_GROUPS, []
        )
        for group in groups:
            folder = source_folder(root, group, arguments.target.resolve())
            destination = group.get(KEY_INSTALL_DEST, scripts_destination)
            for name in group.get(KEY_NAMES, []):
                target = prefix / destination / executable(name)
                if arguments.unlink:
                    unlink(target)
                    linked += 1
                    continue
                if folder is None:
                    continue
                source = folder / executable(name)
                if not source.is_file():
                    print(f"Binary not found: {source}", file=sys.stderr)
                    continue
                link(source, target)
                linked += 1
    print(f"{'Unlinked' if arguments.unlink else 'Linked'} {linked} binaries")
    return 0


if __name__ == "__main__":
    sys.exit(main())
