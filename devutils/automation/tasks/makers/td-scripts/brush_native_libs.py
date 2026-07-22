#
# Copyright 2026 Tabsdata Inc.
#

import importlib
import importlib.util
import os
import sys
import sysconfig
import time
from pathlib import Path
from types import ModuleType

NATIVE_SUFFIXES = (".so", ".dll", ".dylib", ".pyd")
ABI3_SUFFIXES = (".abi3.so", ".abi3.pyd")


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


def tagged_native_libraries_enabled() -> bool:
    return os.getenv("TD_TAGGED_NATIVE_LIBS", "").strip().lower() == "true"


def should_delete(filename: str) -> bool:
    if tagged_native_libraries_enabled():
        return filename.endswith(sysconfig.get_config_var("EXT_SUFFIX"))
    if filename.endswith(ABI3_SUFFIXES):
        return True
    extension_suffix = sysconfig.get_config_var("EXT_SUFFIX")
    if filename.endswith(extension_suffix):
        return False
    return filename.endswith(NATIVE_SUFFIXES)


def delete_with_retry(
    file_path: Path, retries: int = 36, delay_seconds: float = 5.0
) -> None:
    attempt = 0
    while True:
        if not file_path.exists() and not file_path.is_symlink():
            logger.info(f"🚹 File no longer exists: {file_path}. Break.")
            return
        try:
            file_path.unlink()
            logger.info(f"✅ Successfully deleted file: {file_path}")
            return
        except OSError:
            attempt += 1
            if attempt > retries:
                logger.error(f"❌ Failed to delete file after 3 minutes: {file_path}")
                sys.exit(1)
            logger.info(
                f"⏳ Failed to delete {file_path}. Retrying in 5 seconds..."
                f" ({attempt}/{retries})"
            )
            time.sleep(delay_seconds)


def brush(root: Path) -> None:
    if not root.is_dir():
        return
    for extension in NATIVE_SUFFIXES:
        logger.info(f"🧹🧹 Brushing extension: {extension}")
        for file_path in root.rglob(f"*{extension}*"):
            if not file_path.is_file() and not file_path.is_symlink():
                continue
            if not should_delete(file_path.name):
                continue
            logger.info(f"🧹 Brushing file: {file_path}")
            delete_with_retry(file_path)


def main() -> None:
    if len(sys.argv) < 2:
        logger.error("⭕ Usage: brush_native_libs.py <root> [<root> ...]")
        sys.exit(1)
    for argument in sys.argv[1:]:
        brush(Path(argument))


main()
