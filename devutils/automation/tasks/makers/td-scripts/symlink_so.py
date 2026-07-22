#
# Copyright 2025 Tabsdata Inc.
#

import importlib
import importlib.util
import os
import shutil
import sys
import sysconfig
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

NATIVE_SUFFIXES = (".so", ".pyd")
ABI3_SUFFIXES = (".abi3.so", ".abi3.pyd")


def is_native_library(filename: str) -> bool:
    return filename.endswith(NATIVE_SUFFIXES)


def tagged_native_libraries_enabled() -> bool:
    return os.getenv("TD_TAGGED_NATIVE_LIBS", "").strip().lower() == "true"


def tag_native_library_name(filename: str) -> str:
    if not tagged_native_libraries_enabled():
        return filename
    extension_suffix = sysconfig.get_config_var("EXT_SUFFIX")
    if filename.endswith(extension_suffix):
        return filename
    for suffix in ABI3_SUFFIXES:
        if filename.endswith(suffix):
            return filename[: -len(suffix)] + extension_suffix
    for suffix in NATIVE_SUFFIXES:
        if filename.endswith(suffix):
            return filename[: -len(suffix)] + extension_suffix
    return filename


def copy(source: Path, target: Path, link: bool) -> None:
    def ignore_native(_src: str, names: list[str]) -> set[str]:
        return {name for name in names if is_native_library(name)}

    def renaming_copy(
        source_path: str, destination_path: str, *, follow_symlinks: bool = True
    ) -> str:
        destination = Path(destination_path)
        destination = destination.with_name(tag_native_library_name(destination.name))
        if destination.exists() or destination.is_symlink():
            destination.unlink()
        return shutil.copy2(source_path, destination, follow_symlinks=follow_symlinks)

    logger.info("✏️ Copying non-native files...")
    if link:
        shutil.copytree(
            source, target, symlinks=True, dirs_exist_ok=True, ignore=ignore_native
        )
    else:
        shutil.copytree(
            source,
            target,
            symlinks=True,
            dirs_exist_ok=True,
            copy_function=renaming_copy,
        )


def symlink(source: Path, target: Path):
    logger.info("✏️ Hard linking native files...")

    source_absolute = source.resolve()
    target_absolute = target.resolve()

    for root, _, files in os.walk(source_absolute):
        root_path = Path(root)
        for file in files:
            if not is_native_library(file):
                continue
            source_relative = (root_path / file).relative_to(source_absolute)
            target_file = (
                target_absolute / source_relative.parent / tag_native_library_name(file)
            )
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
