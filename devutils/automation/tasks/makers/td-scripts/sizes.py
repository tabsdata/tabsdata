#
# Copyright 2026 Tabsdata Inc.
#

import argparse
import importlib
import importlib.util
import os
import sys
import zipfile
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

MIB = 1024 * 1024
DEFAULT_ENTRY_LIMIT_MIB = 100
DEFAULT_WHEEL_LIMIT_MIB = 200

DIST_PATTERNS = (
    "target/python/dist/*.whl",
    "connectors/python/*/target/python/dist/*.whl",
    "connectors.ee/python/*/target/python/dist/*.whl",
)


def mib(size: int) -> str:
    return f"{size / MIB:.1f} MiB"


def final_wheels(roots: list[Path]) -> list[Path]:
    found: set[Path] = set()
    for root in roots:
        for pattern in DIST_PATTERNS:
            found.update(path.resolve() for path in root.glob(pattern))
    return sorted(found)


def check_wheel(wheel: Path, entry_limit: int, wheel_limit: int) -> list[str]:
    errors: list[str] = []
    size = wheel.stat().st_size
    if size > wheel_limit:
        errors.append(f"wheel {wheel} is {mib(size)}, over {mib(wheel_limit)}")
    with zipfile.ZipFile(wheel) as archive:
        for info in archive.infolist():
            if info.compress_size > entry_limit:
                errors.append(
                    f"{info.filename} in {wheel.name} takes {mib(info.compress_size)}"
                    f" in the wheel ({mib(info.file_size)} unpacked),"
                    f" over {mib(entry_limit)}"
                )
    logger.info(f"{'❌' if errors else '✅'} {mib(size):>10}  {wheel.name}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Fail when a final wheel, or any file inside one (by the size it takes"
            " in the wheel, compressed or stored), is over its limit."
        )
    )
    parser.add_argument(
        "roots",
        nargs="+",
        type=Path,
        help="Repository roots whose target/python/dist folders hold the wheels.",
    )
    parser.add_argument(
        "--entry-limit",
        type=int,
        default=DEFAULT_ENTRY_LIMIT_MIB,
        help="Limit in MiB for the size each file takes inside a wheel.",
    )
    parser.add_argument(
        "--wheel-limit",
        type=int,
        default=DEFAULT_WHEEL_LIMIT_MIB,
        help="Limit in MiB for each wheel.",
    )
    args = parser.parse_args()

    roots = [root.resolve() for root in args.roots if root.is_dir()]
    wheels = final_wheels(roots)
    if not wheels:
        logger.error(
            "❌ No wheels found in the target/python/dist folders of"
            f" {', '.join(str(root) for root in roots)}"
        )
        return 1

    entry_limit = args.entry_limit * MIB
    wheel_limit = args.wheel_limit * MIB
    logger.info(
        f"📏 Checking {len(wheels)} wheel(s): files up to {mib(entry_limit)}"
        f" inside a wheel, wheels up to {mib(wheel_limit)}"
    )
    errors = [
        error
        for wheel in wheels
        for error in check_wheel(wheel, entry_limit, wheel_limit)
    ]
    for error in errors:
        logger.error(f"❌ {error}")
    if errors:
        return 1
    logger.info("✅ All wheels and the files inside them are within their limits")
    return 0


if __name__ == "__main__":
    sys.exit(main())
