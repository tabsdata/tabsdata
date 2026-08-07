#
# Copyright 2026 Tabsdata Inc.
#

import argparse
import configparser
import importlib
import importlib.util
import os
import subprocess
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
sys.stdout.reconfigure(encoding="utf-8")

GO_LICENSES_PACKAGE = "github.com/google/go-licenses@latest"

ALLOWED_LICENSES_SECTION = "allowed licenses"
ALLOWED_MODULES_SECTION = "allowed modules"


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Checks that the licenses of the go modules a go project depends on are "
            "allowed by the given configuration file."
        )
    )
    parser.add_argument(
        "--module",
        action="append",
        required=True,
        help=(
            "Folder holding a go.mod file. Can be repeated to check several "
            "independent go projects."
        ),
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Configuration file holding the allowed licenses and modules.",
    )
    return parser.parse_args()


def read_sections(config_file: str) -> tuple[list[str], list[str]]:
    if not os.path.isfile(config_file):
        logger.error(f"❌ Configuration file not found: {config_file}")
        exit(1)
    parser = configparser.RawConfigParser(allow_no_value=True)
    parser.optionxform = str
    parser.read(config_file, encoding="utf-8")
    licenses = []
    modules = []
    if parser.has_section(ALLOWED_LICENSES_SECTION):
        licenses = [item for item in parser.options(ALLOWED_LICENSES_SECTION) if item]
    if parser.has_section(ALLOWED_MODULES_SECTION):
        modules = [item for item in parser.options(ALLOWED_MODULES_SECTION) if item]
    if not licenses:
        logger.error(f"❌ No allowed licenses declared in {config_file}")
        exit(1)
    return licenses, modules


def module_name(module_folder: str) -> str:
    with open(os.path.join(module_folder, "go.mod"), "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("module "):
                return line.split(maxsplit=1)[1].strip()
    return ""


def check_module(module_folder: str, licenses: list[str], modules: list[str]) -> bool:
    if not os.path.isfile(os.path.join(module_folder, "go.mod")):
        logger.error(f"❌ No go.mod found in module folder {module_folder}")
        return False
    ignored = list(modules)
    own_module = module_name(module_folder)
    if own_module:
        ignored.append(own_module)
    command = [
        "go",
        "run",
        GO_LICENSES_PACKAGE,
        "check",
        "./...",
        "--allowed_licenses",
        ",".join(licenses),
    ]
    for ignore in ignored:
        command.extend(["--ignore", ignore])
    logger.debug(f"🍏 Checking go module: {module_folder}")
    try:
        result = subprocess.run(
            command,
            cwd=module_folder,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except FileNotFoundError:
        logger.error("❌ The go command is required to check the go licenses")
        exit(1)
    if result.returncode != 0:
        logger.error(f"❌ Disallowed go module licenses found in {module_folder}")
        for line in (result.stderr or "").splitlines():
            stripped = line.strip()
            if stripped.startswith(("E", "F")) or "not allowed" in stripped:
                logger.error(f"   {stripped}")
        return False
    logger.info(f"🍏 All go module licenses are allowed in {module_folder}")
    return True


def main():
    arguments = parse_arguments()
    licenses, modules = read_sections(arguments.config)
    failed = 0
    for module_folder in arguments.module:
        if not check_module(module_folder, licenses, modules):
            failed += 1
    if failed:
        logger.error(f"❌ {failed} go project(s) hold disallowed licenses")
        exit(1)


main()
