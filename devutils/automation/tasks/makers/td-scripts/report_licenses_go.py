#
# Copyright 2026 Tabsdata Inc.
#

import argparse
import csv
import importlib
import importlib.util
import io
import os
import subprocess
import sys
from types import ModuleType

from tabulate import tabulate


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

TARGET_DIR = os.path.join(".", "target", "audit")

GO_LICENSES_PACKAGE = "github.com/google/go-licenses@latest"

UNKNOWN_LICENSE = "Unknown License"

normalized_licenses = {
    "": UNKNOWN_LICENSE,
    "Unknown": UNKNOWN_LICENSE,
    "Apache-2.0": "Apache License Version 2.0",
    "BSD-2-Clause": "BSD 2-Clause License",
    "BSD-3-Clause": "BSD 3-Clause License",
    "BSL-1.0": "Boost Software License Version 1.0",
    "CC0-1.0": "Creative Commons Zero Version 1.0 Universal",
    "CC-BY-3.0": "Creative Commons Attribution Version 3.0",
    "CC-BY-4.0": "Creative Commons Attribution Version 4.0",
    "CDLA-Permissive-2.0": "Community Data License Agreement Permissive Version 2.0",
    "ISC": "Internet Systems Consortium License",
    "MIT": "MIT License",
    "MPL-2.0": "Mozilla Public License Version 2.0",
    "OpenSSL": "OpenSSL License",
    "Unlicense": "The Unlicense",
    "Zlib": "zlib License",
}


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Lists the licenses of the go modules a go project depends on, and writes "
            "them as a report in the audit folder of the project."
        )
    )
    parser.add_argument(
        "--module",
        action="append",
        required=True,
        help=(
            "Folder holding a go.mod file. Can be repeated to aggregate several "
            "independent go projects into a single report."
        ),
    )
    parser.add_argument(
        "--name",
        required=False,
        default="",
        help=(
            "Suffix for the report files, so that independent projects do not "
            "overwrite each other. With 'km', the report is 'licenses_km_go.txt'."
        ),
    )
    return parser.parse_args()


def normalized_license(name: str) -> str:
    return normalized_licenses.get(name.strip(), name.strip())


def module_name(module_folder: str) -> str:
    with open(os.path.join(module_folder, "go.mod"), "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("module "):
                return line.split(maxsplit=1)[1].strip()
    return ""


def module_licenses(module_folder: str) -> list[list[str]]:
    if not os.path.isfile(os.path.join(module_folder, "go.mod")):
        logger.error(f"❌ No go.mod found in module folder {module_folder}")
        return []
    logger.debug(f"🍏 Processing go module: {module_folder}")
    command = [
        "go",
        "run",
        GO_LICENSES_PACKAGE,
        "csv",
        "./...",
    ]
    own_module = module_name(module_folder)
    if own_module:
        command.extend(["--ignore", own_module])
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
        logger.error("❌ The go command is required to report the go licenses")
        exit(1)
    if result.returncode != 0:
        logger.warn(
            f"🪹 go-licenses could not classify every module in {module_folder}."
            " The unresolved ones are reported as unknown."
        )
        for line in (result.stderr or "").splitlines():
            if line.strip().startswith(("E", "F")):
                logger.debug(f"🔸 {line.strip()}")
    rows = []
    reader = csv.reader(io.StringIO(result.stdout or ""))
    for row in reader:
        if len(row) < 3 or not row[0].strip():
            continue
        rows.append([row[0].strip(), "", normalized_license(row[2])])
    return rows


def main():
    arguments = parse_arguments()

    aggregated = {}
    for module_folder in arguments.module:
        for name, version, license_name in module_licenses(module_folder):
            if name in aggregated:
                continue
            aggregated[name] = [name, version, license_name]

    if not aggregated:
        logger.warn("🪹 No go module licenses were resolved. Writing an empty report.")

    data = sorted(aggregated.values(), key=lambda row: (row[2], row[0]))

    suffix = f"_{arguments.name}" if arguments.name else ""
    target_file = os.path.join(TARGET_DIR, f"licenses{suffix}_go.txt")
    target_csv = os.path.join(TARGET_DIR, f"licenses{suffix}_go.csv")

    headers = ["Name", "Version", "License"]
    content = (
        "This project includes code from the following Go modules and versions"
        " (grouped by license):\n\n"
    )
    content += tabulate(data, headers=headers, tablefmt="fancy_grid")

    os.makedirs(TARGET_DIR, exist_ok=True)
    with open(target_file, "w", encoding="utf-8") as f:
        f.write(content + "\n")
    with open(target_csv, "w", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(data)

    logger.info(f"🍏 Reported {len(data)} go module license(s) into {target_file}")


main()
