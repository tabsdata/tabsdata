#
# Copyright 2025 Tabs Data Inc.
#

import importlib
import importlib.util
import os
import shutil
import sys
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


def copy(source: Path, target: Path, link: bool) -> None:

    def ignore_so(_src: str, names: list[str]) -> set[str]:
        return {name for name in names if name.endswith(".so")}

    logger.info("✏️ Copying non-.so files...")
    if link:
        shutil.copytree(
            source, target, symlinks=True, dirs_exist_ok=True, ignore=ignore_so
        )
    else:
        for so_file in target.rglob("*.so"):
            if so_file.is_file():
                so_file.unlink()
        shutil.copytree(source, target, symlinks=True, dirs_exist_ok=True)


def symlink(source: Path, target: Path):
    logger.info("✏️ Hard linking .so files...")

    source_absolute = source.resolve()
    target_absolute = target.resolve()

    for so_file in target_absolute.rglob("*.so"):
        if so_file.is_file():
            so_file.unlink()

    for root, _, files in os.walk(source_absolute):
        root_path = Path(root)
        for file in files:
            if not file.endswith(".so"):
                continue
            source_file = (root_path / file).relative_to(source_absolute)
            target_file = (target_absolute / source_file).resolve()
            source_file = (root_path / file).resolve()
            target_file.parent.mkdir(parents=True, exist_ok=True)
            try:
                logger.info(f"🔗 Hard linking {source_file} to {target_file}")
                if target_file.exists() or target_file.is_symlink():
                    target_file.unlink()
                os.symlink(source_file, target_file)
            except Exception as e:
                logger.error(
                    f"⭕ Error sym linking {source_file} to {target_file}: {e}"
                )
                exit(1)


def symlink_so():
    if len(sys.argv) != 4:
        logger.error("⭕ Usage: symlink_so.py <source> <target>")
        exit(1)

    source = Path(sys.argv[1])
    target = Path(sys.argv[2])
    link = sys.argv[3].strip().lower() == "true"

    if not source.is_dir():
        logger.error️(f"⭕ Source directory not found: {source}")
        exit(1)

    copy(source, target, link)

    if link:
        symlink(source, target)


symlink_so()
