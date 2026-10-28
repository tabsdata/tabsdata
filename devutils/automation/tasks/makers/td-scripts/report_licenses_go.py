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

# fmt: off
# noinspection DuplicatedCode
normalized_licenses = {
    "": "Unspecified License",                                                                                                                                                                 # noqa: E231,E241,E501
    "(Apache-2.0 OR ISC OR MIT) AND (Apache-2.0 OR ISC OR MIT-0) AND (Apache-2.0 OR ISC) AND Apache-2.0 AND BSD-3-Clause AND ISC AND MIT":                                                     # noqa: E231,E241,E501
        "Apache License Version 2.0 or Internet Systems Consortium License or MIT License, and "                                                                                               # noqa: E231,E241,E501
        "Apache License Version 2.0 or Internet Systems Consortium License or MIT No Attribution License, and "                                                                                # noqa: E231,E241,E501
        "Apache License Version 2.0 or Internet Systems Consortium License, and "                                                                                                              # noqa: E231,E241,E501
        "Apache License Version 2.0, and BSD Revised License (BSD-3-Clause), and "                                                                                                             # noqa: E231,E241,E501
        "Internet Systems Consortium License, and MIT License",                                                                                                                                # noqa: E231,E241,E501
    "(Apache-2.0 OR ISC) AND ISC AND OpenSSL": "Apache License Version 2.0 or Internet Systems Consortium License, and Internet Systems Consortium License, and OpenSSL License",              # noqa: E231,E241,E501
    "(Apache-2.0 OR ISC) AND ISC": "Apache License Version 2.0 or Internet Systems Consortium License, and Internet Systems Consortium License",                                               # noqa: E231,E241,E501
    "(Apache-2.0 OR MIT) AND BSD-3-Clause": "Apache License Version 2.0 or MIT License, and BSD Revised License (BSD-3-Clause)",                                                               # noqa: E231,E241,E501
    "(Apache-2.0 OR MIT) AND PostgreSQL": "Apache License Version 2.0 or MIT License, and PostgreSQL License",                                                                                 # noqa: E231,E241,E501
    "(Apache-2.0 OR MIT) AND Unicode-3.0": "Apache License Version 2.0 or MIT License, and Unicode License Version 3.0",                                                                       # noqa: E231,E241,E501
    "(Apache-2.0 OR MIT) AND Unicode-DFS-2016": "Apache License Version 2.0 or MIT License, and Unicode Data Files and Software 2016",                                                         # noqa: E231,E241,E501
    "(MIT OR Apache-2.0)": "MIT License or Apache License Version 2.0",                                                                                                                        # noqa: E231,E241,E501
    "(MIT OR Apache-2.0) AND Unicode-3.0": "MIT License or Apache License Version 2.0, and Unicode License Version 3.0",                                                                       # noqa: E231,E241,E501
    "(MIT OR CC0-1.0)": "MIT License, and Creative Commons Zero Version 1.0 Universal",                                                                                                        # noqa: E231,E241,E501
    "0BSD OR Apache-2.0 OR MIT": "BSD Zero-Clause License or Apache License Version 2.0 or MIT License",                                                                                       # noqa: E231,E241,E501
    "0BSD;; BSD-3-CLAUSE;; CC0-1.0;; MIT;; ZLIB": "BSD Zero-Clause License or BSD Revised License (BSD-3-Clause) or Creative Commons Zero Version 1.0 Universal or MIT License",               # noqa: E231,E241,E501
    "0BSD": "BSD Zero-Clause License",                                                                                                                                                         # noqa: E231,E241,E501
    "3-CLAUSE BSD LICENSE": "BSD Revised License (BSD-3-Clause)",                                                                                                                              # noqa: E231,E241,E501
    "APACHE 2": "Apache License Version 2.0",                                                                                                                                                  # noqa: E231,E241,E501
    "APACHE 2.0": "Apache License Version 2.0",                                                                                                                                                # noqa: E231,E241,E501
    "APACHE 2.0 LICENSE": "Apache License Version 2.0",                                                                                                                                        # noqa: E231,E241,E501
    "APACHE LICENSE 2.0": "Apache License Version 2.0",                                                                                                                                        # noqa: E231,E241,E501
    "APACHE LICENSE_ VERSION 2.0": "Apache License Version 2.0",                                                                                                                               # noqa: E231,E241,E501
    "APACHE SOFTWARE LICENSE V2": "Apache License Version 2.0",                                                                                                                                # noqa: E231,E241,E501
    "APACHE SOFTWARE LICENSE;; BSD LICENSE": "Apache License Version 2.0 or BSD License",                                                                                                      # noqa: E231,E241,E501
    "APACHE SOFTWARE LICENSE;; MIT LICENSE;; MOZILLA PUBLIC LICENSE 2.0 _MPL 2.0_": "Apache License Version 2.0 or MIT License or Mozilla Public License Version 2.0",                         # noqa: E231,E241,E501
    "APACHE SOFTWARE LICENSE;; MIT LICENSE": "Apache License Version 2.0 or MIT License",                                                                                                      # noqa: E231,E241,E501
    "APACHE SOFTWARE LICENSE": "Apache License Version 2.0",                                                                                                                                   # noqa: E231,E241,E501
    "Apache-2.0 AND ISC": "Apache License Version 2.0, and Internet Systems Consortium License",                                                                                               # noqa: E231,E241,E501
    "Apache-2.0 AND MIT": "Apache License Version 2.0, and MIT License",                                                                                                                       # noqa: E231,E241,E501
    "Apache-2.0 OR Apache-2.0 WITH LLVM-exception OR CC0-1.0": "Apache License Version 2.0 or Apache License Version 2.0 with LLVM Exception or Creative Commons Zero Version 1.0 Universal",  # noqa: E231,E241,E501
    "Apache-2.0 OR Apache-2.0 WITH LLVM-exception OR MIT": "Apache License Version 2.0 or Apache License Version 2.0 with LLVM Exception or MIT License",                                      # noqa: E231,E241,E501
    "Apache-2.0 OR BSD-2-Clause OR MIT": "Apache License Version 2.0 or BSD Simplified License (BSD-2-Clause) or MIT License",                                                                 # noqa: E231,E241,E501
    "Apache-2.0 OR BSD-2-Clause": "Apache License Version 2.0 or BSD Simplified License (BSD-2-Clause)",                                                                                       # noqa: E231,E241,E501
    "Apache-2.0 OR BSD-3-Clause": "Apache License Version 2.0 or BSD Revised License (BSD-3-Clause)",                                                                                          # noqa: E231,E241,E501
    "Apache-2.0 OR BSL-1.0 OR MIT": "Apache License Version 2.0 or Boost Software License Version 1.0 or MIT License",                                                                         # noqa: E231,E241,E501
    "Apache-2.0 OR BSL-1.0": "Apache License Version 2.0 or Boost Software License Version 1.0",                                                                                               # noqa: E231,E241,E501
    "Apache-2.0 OR CC0-1.0 OR MIT-0": "Apache License Version 2.0 or Creative Commons Zero Version 1.0 Universal or MIT No Attribution License",                                               # noqa: E231,E241,E501
    "Apache-2.0 OR GPL-2.0": "Apache License Version 2.0 or GNU General Public License Version 2.0",                                                                                           # noqa: E231,E241,E501
    "Apache-2.0 OR ISC OR MIT": "Apache License Version 2.0 or Internet Systems Consortium License or MIT License",                                                                            # noqa: E231,E241,E501
    "Apache-2.0 OR LGPL-2.1-or-later OR MIT": "Apache License Version 2.0 or GNU Lesser General Public License Version 2.1 or later or MIT License",                                           # noqa: E231,E241,E501
    "Apache-2.0 OR MIT OR Zlib": "Apache License Version 2.0 or MIT License or Zlib License",                                                                                                  # noqa: E231,E241,E501
    "Apache-2.0 OR MIT": "Apache License Version 2.0 or MIT License",                                                                                                                          # noqa: E231,E241,E501
    "Apache-2.0 WITH LLVM-exception": "Apache License Version 2.0 with LLVM Exception",                                                                                                        # noqa: E231,E241,E501
    "Apache-2.0;; Apache-2.0 WITH LLVM-exception;; BSD-2-Clause;; BSD-3-Clause;; BSL-1.0;; MIT": "Apache License Version 2.0 or Apache License Version 2.0 with LLVM Exception or "            # noqa: E231,E241,E501
        "BSD Simplified License (BSD-2-Clause) or BSD Revised License (BSD-3-Clause) or "                                                                                                      # noqa: E231,E241,E501
        "Boost Software License Version 1.0 or MIT License",                                                                                                                                   # noqa: E231,E241,E501
    "Apache-2.0;; Apache-2.0 WITH LLVM-exception;; BSD-2-Clause;; BSD-3-Clause;; BSL-1.0;; ISC;; MIT;; Zlib":                                                                                  # noqa: E231,E241,E501
        "Apache License Version 2.0 or Apache License Version 2.0 with LLVM Exception or "                                                                                                     # noqa: E231,E241,E501
        "BSD Simplified License (BSD-2-Clause) or BSD Revised License (BSD-3-Clause) or "                                                                                                      # noqa: E231,E241,E501
        "Boost Software License Version 1.0 or Internet Systems Consortium License or MIT License or "                                                                                         # noqa: E231,E241,E501
        "Zlib License",                                                                                                                                                                        # noqa: E231,E241,E501
    "APACHE-2.0;; BSD 3-CLAUSE": "Apache License Version 2.0 or BSD Revised License (BSD-3-Clause)",                                                                                           # noqa: E231,E241,E501
    "APACHE-2.0;; BSD-2-CLAUSE": "Apache License Version 2.0 or BSD Simplified License (BSD-2-Clause)",                                                                                        # noqa: E231,E241,E501
    "APACHE-2.0;; BSD-3-CLAUSE": "Apache License Version 2.0 or BSD Revised License (BSD-3-Clause)",                                                                                           # noqa: E231,E241,E501
    "APACHE-2.0;; CNRI-PYTHON": "Apache License Version 2.0 or Python Software Foundation License Version 2.0",                                                                                # noqa: E231,E241,E501
    "APACHE-2.0;; MIT": "Apache License Version 2.0 or MIT License",                                                                                                                           # noqa: E231,E241,E501
    "APACHE-2.0;; UPL-1.0": "Apache License Version 2.0 or Universal Permissive License Version 1.0",                                                                                          # noqa: E231,E241,E501
    "Apache-2.0": "Apache License Version 2.0",                                                                                                                                                # noqa: E231,E241,E501
    "APACHE-2.0": "Apache License Version 2.0",                                                                                                                                                # noqa: E231,E241,E501
    "Artistic-2.0 OR CC0-1.0": "Artistic License Version 2.0 or Creative Commons Zero Version 1.0 Universal",                                                                                  # noqa: E231,E241,E501
    "BlueOak-1.0.0": "Blue Oak Model License Version 1.0.0",                                                                                                                                   # noqa: E231,E241,E501
    "BSD 3-CLAUSE": "BSD Revised License (BSD-3-Clause)",                                                                                                                                      # noqa: E231,E241,E501
    "BSD LICENSE;; APACHE SOFTWARE LICENSE": "BSD Revised License (BSD-3-Clause) or Apache Software License Version 2.0",                                                                      # noqa: E231,E241,E501
    "BSD LICENSE;; PUBLIC DOMAIN": "BSD Revised License (BSD-3-Clause) or Public Domain",                                                                                                      # noqa: E231,E241,E501
    "BSD LICENSE": "BSD Revised License (BSD-3-Clause)",                                                                                                                                       # noqa: E231,E241,E501
    "BSD-2-Clause": "BSD Simplified License (BSD-2-Clause)",                                                                                                                                   # noqa: E231,E241,E501
    "BSD-2-CLAUSE": "BSD Simplified License (BSD-2-Clause)",                                                                                                                                   # noqa: E231,E241,E501
    "BSD-3-Clause AND MIT": "BSD Revised License (BSD-3-Clause), and MIT License",                                                                                                             # noqa: E231,E241,E501
    "BSD-3-Clause OR MIT": "BSD Revised License (BSD-3-Clause) or MIT License",                                                                                                                # noqa: E231,E241,E501
    "BSD-3-CLAUSE;; ISC": "BSD Revised License (BSD-3-Clause) or Internet Systems Consortium License",                                                                                         # noqa: E231,E241,E501
    "BSD-3-CLAUSE;; MIT": "BSD Revised License (BSD-3-Clause) or MIT License",                                                                                                                 # noqa: E231,E241,E501
    "BSD-3-Clause": "BSD Revised License (BSD-3-Clause)",                                                                                                                                      # noqa: E231,E241,E501
    "BSD-3-CLAUSE": "BSD Revised License (BSD-3-Clause)",                                                                                                                                      # noqa: E231,E241,E501
    "BSD": "BSD Revised License (BSD-3-Clause)",                                                                                                                                               # noqa: E231,E241,E501
    "BSL-1.0": "Boost Software License Version 1.0",                                                                                                                                           # noqa: E231,E241,E501
    "bzip2-1.0.6": "bzip2 and libbzip2 License, Version 1.0.6",                                                                                                                                # noqa: E231,E241,E501
    "CC-BY-3.0": "Creative Commons Attribution Version 3.0 Unported License",                                                                                                                  # noqa: E231,E241,E501
    "CC-BY-4.0": "Creative Commons Attribution Version 4.0 International License",                                                                                                             # noqa: E231,E241,E501
    "CC0-1.0": "Creative Commons Zero Version 1.0 Universal",                                                                                                                                  # noqa: E231,E241,E501
    "CDLA-Permissive-2.0": "Community Data License Agreement Permissive Version 2.0",                                                                                                          # noqa: E231,E241,E501
    "CMU LICENSE (MIT-CMU)": "MIT License (CMU Variant)",                                                                                                                                      # noqa: E231,E241,E501
    "GNU GENERAL PUBLIC LICENSE _GPL_": "GNU General Public License (GPL)",                                                                                                                    # noqa: E231,E241,E501
    "GNU LIBRARY;; LESSER GENERAL PUBLIC LICENSE _LGPL_": "GNU Lesser General Public License (LGPL)",                                                                                          # noqa: E231,E241,E501
    "ISC AND (Apache-2.0 OR ISC) AND OpenSSL": "Internet Systems Consortium License, and Apache License Version 2.0 or Internet Systems Consortium License, and OpenSSL License",              # noqa: E231,E241,E501
    "ISC AND (Apache-2.0 OR ISC)": "Internet Systems Consortium License, and Apache License Version 2.0 or Internet Systems Consortium License",                                               # noqa: E231,E241,E501
    "ISC LICENSE _ISCL_": "Internet Systems Consortium License",                                                                                                                               # noqa: E231,E241,E501
    "ISC": "Internet Systems Consortium License",                                                                                                                                              # noqa: E231,E241,E501
    "LICENSEREF-NVIDIA-CUFILE-LICENSE": "NVIDIA Proprietary License",                                                                                                                          # noqa: E231,E241,E501
    "LICENSEREF-NVIDIA-CUSPARSELT-LICENSE": "NVIDIA Proprietary License",                                                                                                                      # noqa: E231,E241,E501
    "LICENSEREF-NVIDIA-PROPRIETARY": "NVIDIA Proprietary License",                                                                                                                             # noqa: E231,E241,E501
    "LICENSEREF-NVIDIA-SOFTWARE-LICENSE": "NVIDIA Proprietary License",                                                                                                                        # noqa: E231,E241,E501
    "MIT AND Apache-2.0": "MIT License, and Apache License Version 2.0",                                                                                                                       # noqa: E231,E241,E501
    "MIT AND BSD-3-Clause": "MIT License, and BSD Revised License (BSD-3-Clause)",                                                                                                             # noqa: E231,E241,E501
    "MIT AND Unicode-DFS-2016": "MIT License, and Unicode Data Files and Software 2016",                                                                                                       # noqa: E231,E241,E501
    "MIT LICENSE_ APACHE LICENSE_ VERSION 2.0": "MIT License or Apache License Version 2.0",                                                                                                   # noqa: E231,E241,E501
    "MIT LICENSE;; APACHE SOFTWARE LICENSE": "MIT License or Apache Software License Version 2.0",                                                                                             # noqa: E231,E241,E501
    "MIT LICENSE;; MOZILLA PUBLIC LICENSE 2.0 _MPL 2.0_": "MIT License or Mozilla Public License Version 2.0",                                                                                 # noqa: E231,E241,E501
    "MIT LICENSE": "MIT License",                                                                                                                                                              # noqa: E231,E241,E501
    "MIT NO ATTRIBUTION LICENSE _MIT-0_": "MIT No Attribution License",                                                                                                                        # noqa: E231,E241,E501
    "MIT OR Apache2": "MIT License or Apache License Version 2.0",                                                                                                                             # noqa: E231,E241,E501
    "MIT OR MPL-2.0": "MIT License or Mozilla Public License Version 2.0",                                                                                                                     # noqa: E231,E241,E501
    "MIT OR Unlicense": "MIT License or The Unlicense",                                                                                                                                        # noqa: E231,E241,E501
    "MIT-0": "MIT No Attribution License",                                                                                                                                                     # noqa: E231,E241,E501
    "MIT-CMU": "MIT License (CMU Variant)",                                                                                                                                                    # noqa: E231,E241,E501
    "MIT;; MPL-2.0": "MIT License or Mozilla Public License Version 2.0",                                                                                                                      # noqa: E231,E241,E501
    "MIT;; PSF-2.0": "MIT License or Python Software Foundation License Version 2.0",                                                                                                          # noqa: E231,E241,E501
    "MIT;; PYTHON-2.0": "MIT License or Python Software Foundation License Version 2.0",                                                                                                       # noqa: E231,E241,E501
    "MIT_;; MPL-2.0;; _Apache-2.0": "MIT License or Mozilla Public License Version 2.0 or Apache License Version 2.0",                                                                         # noqa: E231,E241,E501
    "MIT": "MIT License",                                                                                                                                                                      # noqa: E231,E241,E501
    "MOZILLA PUBLIC LICENSE 2.0 _MPL 2.0_": "Mozilla Public License Version 2.0",                                                                                                              # noqa: E231,E241,E501
    "MOZILLA PUBLIC LICENSE 2.0 (MPL 2.0)": "Mozilla Public License Version 2.0",                                                                                                              # noqa: E231,E241,E501
    "MPL-2.0": "Mozilla Public License Version 2.0",                                                                                                                                           # noqa: E231,E241,E501
    "NVIDIA PROPRIETARY SOFTWARE": "NVIDIA Proprietary License",                                                                                                                               # noqa: E231,E241,E501
    "OpenSSL": "OpenSSL License",                                                                                                                                                              # noqa: E231,E241,E501
    "OTHER_PROPRIETARY LICENSE": "Apache License Version 2.0 or MIT License or NVIDIA Proprietary License",                                                                                    # noqa: E231,E241,E501
    "PSF-2.0": "Python Software Foundation License Version 2.0",                                                                                                                               # noqa: E231,E241,E501
    "PYTHON SOFTWARE FOUNDATION LICENSE": "Python Software Foundation License Version 2.0",                                                                                                    # noqa: E231,E241,E501
    "Python-2.0": "Python Software Foundation License Version 2.0",                                                                                                                            # noqa: E231,E241,E501
    "TabsData License": "TabsData License",                                                                                                                                                    # noqa: E231,E241,E501
    "THE UNLICENSE _UNLICENSE_": "The Unlicense",                                                                                                                                              # noqa: E231,E241,E501
    "THE UNLICENSE (UNLICENSE)": "The Unlicense",                                                                                                                                              # noqa: E231,E241,E501
    "Unicode-3.0": "Unicode License Version 3.0",                                                                                                                                              # noqa: E231,E241,E501
    "UNKNOWN": "The Unknown License",                                                                                                                                                          # noqa: E231,E241,E501
    "Unlicense": "The Unlicense",                                                                                                                                                              # noqa: E231,E241,E501
    "UNLICENSED": "The Unlicense",                                                                                                                                                             # noqa: E231,E241,E501
    "WTFPL": "Do What The F*ck You Want To Public License",                                                                                                                                    # noqa: E231,E241,E501
    "Zlib": "Zlib License",                                                                                                                                                                    # noqa: E231,E241,E501
}
# fmt: on


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
    key = name.strip()
    if key in normalized_licenses:
        return normalized_licenses[key]
    key_lower = key.lower()
    for normalized_key, normalized_value in normalized_licenses.items():
        if normalized_key.lower() == key_lower:
            return normalized_value
    return key


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
