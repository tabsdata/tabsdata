#
# Copyright 2025 Tabs Data Inc.
#

import csv
import importlib
import importlib.util
import io
import json
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
TARGET_FILE = os.path.join(TARGET_DIR, "licenses_rs.txt")

# fmt: off
# noinspection DuplicatedCode
normalized_licenses = {
    "": "Unspecified License",                                                                                                                                                                 # noqa: E231,E241,E501
    "(Apache-2.0 OR ISC) AND ISC AND OpenSSL": "Apache License Version 2.0 or Internet Systems Consortium License, and Internet Systems Consortium License, and OpenSSL License",              # noqa: E231,E241,E501
    "(Apache-2.0 OR ISC) AND ISC": "Apache License Version 2.0 or Internet Systems Consortium License, and Internet Systems Consortium License",                                               # noqa: E231,E241,E501
    "(Apache-2.0 OR MIT) AND BSD-3-Clause": "Apache License Version 2.0 or MIT License, and BSD Revised License (BSD-3-Clause)",                                                               # noqa: E231,E241,E501
    "(Apache-2.0 OR MIT) AND Unicode-3.0": "Apache License Version 2.0 or MIT License, and Unicode License Version 3.0",                                                                       # noqa: E231,E241,E501
    "(MIT OR Apache-2.0) AND Unicode-3.0": "MIT License or Apache License Version 2.0, and Unicode License Version 3.0",                                                                       # noqa: E231,E241,E501
    "(MIT OR CC0-1.0)": "MIT License, and Creative Commons Zero Version 1.0 Universal",                                                                                                        # noqa: E231,E241,E501
    "0BSD OR Apache-2.0 OR MIT": "BSD Zero-Clause License or Apache License Version 2.0 or MIT License",                                                                                       # noqa: E231,E241,E501
    "0BSD": "BSD Zero-Clause License",                                                                                                                                                         # noqa: E231,E241,E501
    "3-CLAUSE BSD LICENSE": "BSD Revised License (BSD-3-Clause)",                                                                                                                              # noqa: E231,E241,E501
    "APACHE 2.0": "Apache License Version 2.0",                                                                                                                                                # noqa: E231,E241,E501
    "APACHE LICENSE 2.0": "Apache License Version 2.0",                                                                                                                                        # noqa: E231,E241,E501
    "APACHE LICENSE_ VERSION 2.0": "Apache License Version 2.0",                                                                                                                               # noqa: E231,E241,E501
    "APACHE SOFTWARE LICENSE V2": "Apache License Version 2.0",                                                                                                                                # noqa: E231,E241,E501
    "APACHE SOFTWARE LICENSE;; BSD LICENSE": "Apache License Version 2.0 or BSD License",                                                                                                      # noqa: E231,E241,E501
    "APACHE SOFTWARE LICENSE;; MIT LICENSE": "Apache License Version 2.0 or MIT License",                                                                                                      # noqa: E231,E241,E501
    "APACHE SOFTWARE LICENSE": "Apache License Version 2.0",                                                                                                                                   # noqa: E231,E241,E501
    "Apache-2.0 AND ISC": "Apache License Version 2.0, and Internet Systems Consortium License",                                                                                               # noqa: E231,E241,E501
    "Apache-2.0 AND MIT": "Apache License Version 2.0, and MIT License",                                                                                                                       # noqa: E231,E241,E501
    "Apache-2.0 OR Apache-2.0 WITH LLVM-exception OR CC0-1.0": "Apache License Version 2.0 or Apache License Version 2.0 with LLVM Exception or Creative Commons Zero Version 1.0 Universal",  # noqa: E231,E241,E501
    "Apache-2.0 OR Apache-2.0 WITH LLVM-exception OR MIT": "Apache License Version 2.0 or Apache License Version 2.0 with LLVM Exception or MIT License",                                      # noqa: E231,E241,E501
    "Apache-2.0 OR BSD-2-Clause OR MIT": "Apache License Version 2.0 or BSD Simplified License (BSD-2-Clause) or MIT License",                                                                 # noqa: E231,E241,E501
    "Apache-2.0 OR BSL-1.0 OR MIT": "Apache License Version 2.0 or Boost Software License Version 1.0 or MIT License",                                                                         # noqa: E231,E241,E501
    "Apache-2.0 OR BSL-1.0": "Apache License Version 2.0 or Boost Software License Version 1.0",                                                                                               # noqa: E231,E241,E501
    "Apache-2.0 OR CC0-1.0 OR MIT-0": "Apache License Version 2.0 or Creative Commons Zero Version 1.0 Universal or MIT No Attribution License",                                               # noqa: E231,E241,E501
    "Apache-2.0 OR ISC OR MIT": "Apache License Version 2.0 or Internet Systems Consortium License or MIT License",                                                                            # noqa: E231,E241,E501
    "Apache-2.0 OR LGPL-2.1-or-later OR MIT": "Apache License Version 2.0 or GNU Lesser General Public License Version 2.1 or later or MIT License",                                           # noqa: E231,E241,E501
    "Apache-2.0 OR MIT OR Zlib": "Apache License Version 2.0 or MIT License or Zlib License",                                                                                                  # noqa: E231,E241,E501
    "Apache-2.0 OR MIT": "Apache License Version 2.0 or MIT License",                                                                                                                          # noqa: E231,E241,E501
    "Apache-2.0 WITH LLVM-exception": "Apache License Version 2.0 with LLVM Exception",                                                                                                        # noqa: E231,E241,E501
    "APACHE-2.0;; BSD 3-CLAUSE": "Apache License Version 2.0 or BSD Revised License (BSD-3-Clause)",                                                                                           # noqa: E231,E241,E501
    "APACHE-2.0;; BSD-2-CLAUSE": "Apache License Version 2.0 or BSD Simplified License (BSD-2-Clause)",                                                                                        # noqa: E231,E241,E501
    "APACHE-2.0;; BSD-3-CLAUSE": "Apache License Version 2.0 or BSD Revised License (BSD-3-Clause)",                                                                                           # noqa: E231,E241,E501
    "APACHE-2.0;; CNRI-PYTHON": "Apache License Version 2.0 or Python Software Foundation License Version 2.0",                                                                                # noqa: E231,E241,E501
    "APACHE-2.0;; MIT": "Apache License Version 2.0 or MIT License",                                                                                                                           # noqa: E231,E241,E501
    "Apache-2.0": "Apache License Version 2.0",                                                                                                                                                # noqa: E231,E241,E501
    "APACHE-2.0": "Apache License Version 2.0",                                                                                                                                                # noqa: E231,E241,E501
    "Artistic-2.0 OR CC0-1.0": "Artistic License Version 2.0 or Creative Commons Zero Version 1.0 Universal",                                                                                  # noqa: E231,E241,E501
    "BlueOak-1.0.0": "Blue Oak Model License Version 1.0.0",                                                                                                                                   # noqa: E231,E241,E501
    "BSD 3-CLAUSE": "BSD Revised License (BSD-3-Clause)",                                                                                                                                      # noqa: E231,E241,E501
    "BSD LICENSE;; APACHE SOFTWARE LICENSE": "BSD Revised License (BSD-3-Clause) or Apache Software License Version 2.0",                                                                      # noqa: E231,E241,E501
    "BSD LICENSE;; PUBLIC DOMAIN": "BSD Revised License (BSD-3-Clause) or Public Domain",                                                                                                      # noqa: E231,E241,E501
    "BSD LICENSE": "BSD Revised License (BSD-3-Clause)",                                                                                                                                       # noqa: E231,E241,E501
    "BSD-2-Clause": "BSD Simplified License (BSD-2-Clause)",                                                                                                                                   # noqa: E231,E241,E501
    "BSD-3-Clause AND MIT": "BSD Revised License (BSD-3-Clause), and MIT License",                                                                                                             # noqa: E231,E241,E501
    "BSD-3-Clause OR MIT": "BSD Revised License (BSD-3-Clause) or MIT License",                                                                                                                # noqa: E231,E241,E501
    "BSD-3-CLAUSE;; ISC": "BSD Revised License (BSD-3-Clause) or Internet Systems Consortium License",                                                                                         # noqa: E231,E241,E501
    "BSD-3-CLAUSE;; MIT": "BSD Revised License (BSD-3-Clause) or MIT License",                                                                                                                 # noqa: E231,E241,E501
    "BSD-3-Clause": "BSD Revised License (BSD-3-Clause)",                                                                                                                                      # noqa: E231,E241,E501
    "BSD-3-CLAUSE": "BSD Revised License (BSD-3-Clause)",                                                                                                                                      # noqa: E231,E241,E501
    "BSD": "BSD Revised License (BSD-3-Clause)",                                                                                                                                               # noqa: E231,E241,E501
    "BSL-1.0": "Boost Software License Version 1.0",                                                                                                                                           # noqa: E231,E241,E501
    "CC-BY-4.0": "Creative Commons Attribution Version 4.0 International License",                                                                                                             # noqa: E231,E241,E501
    "CC0-1.0": "Creative Commons Zero Version 1.0 Universal",                                                                                                                                  # noqa: E231,E241,E501
    "CDLA-Permissive-2.0": "Community Data License Agreement Permissive Version 2.0",                                                                                                          # noqa: E231,E241,E501
    "CMU LICENSE (MIT-CMU)": "MIT License (CMU Variant)",                                                                                                                                      # noqa: E231,E241,E501
    "ISC AND (Apache-2.0 OR ISC) AND OpenSSL": "Internet Systems Consortium License, and Apache License Version 2.0 or Internet Systems Consortium License, and OpenSSL License",              # noqa: E231,E241,E501
    "ISC AND (Apache-2.0 OR ISC)": "Internet Systems Consortium License, and Apache License Version 2.0 or Internet Systems Consortium License",                                               # noqa: E231,E241,E501
    "ISC LICENSE _ISCL_": "Internet Systems Consortium License",                                                                                                                               # noqa: E231,E241,E501
    "ISC": "Internet Systems Consortium License",                                                                                                                                              # noqa: E231,E241,E501
    "MIT AND Apache-2.0": "MIT License, and Apache License Version 2.0",                                                                                                                       # noqa: E231,E241,E501
    "MIT AND BSD-3-Clause": "MIT License, and BSD Revised License (BSD-3-Clause)",                                                                                                             # noqa: E231,E241,E501
    "MIT LICENSE_ APACHE LICENSE_ VERSION 2.0": "MIT License or Apache License Version 2.0",                                                                                                   # noqa: E231,E241,E501
    "MIT LICENSE;; APACHE SOFTWARE LICENSE": "MIT License or Apache Software License Version 2.0",                                                                                             # noqa: E231,E241,E501
    "MIT LICENSE;; MOZILLA PUBLIC LICENSE 2.0 _MPL 2.0_": "MIT License or Mozilla Public License Version 2.0",                                                                                 # noqa: E231,E241,E501
    "MIT LICENSE": "MIT License",                                                                                                                                                              # noqa: E231,E241,E501
    "MIT NO ATTRIBUTION LICENSE _MIT-0_": "MIT No Attribution License",                                                                                                                        # noqa: E231,E241,E501
    "MIT OR Unlicense": "MIT License or The Unlicense",                                                                                                                                        # noqa: E231,E241,E501
    "MIT-0": "MIT No Attribution License",                                                                                                                                                     # noqa: E231,E241,E501
    "MIT-CMU": "MIT License (CMU Variant)",                                                                                                                                                    # noqa: E231,E241,E501
    "MIT;; PYTHON-2.0": "MIT License or Python Software Foundation License Version 2.0",                                                                                                       # noqa: E231,E241,E501
    "MIT": "MIT License",                                                                                                                                                                      # noqa: E231,E241,E501
    "MOZILLA PUBLIC LICENSE 2.0 _MPL 2.0_": "Mozilla Public License Version 2.0",                                                                                                              # noqa: E231,E241,E501
    "MOZILLA PUBLIC LICENSE 2.0 (MPL 2.0)": "Mozilla Public License Version 2.0",                                                                                                              # noqa: E231,E241,E501
    "MPL-2.0": "Mozilla Public License Version 2.0",                                                                                                                                           # noqa: E231,E241,E501
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
    "Zlib": "Zlib License",                                                                                                                                                                    # noqa: E231,E241,E501
}
# fmt: on


def get_custom_license(_c_name, c_license):
    key = c_license.strip()
    if key in normalized_licenses:
        return normalized_licenses[key]
    logger.error(f"No label for crate license '{c_license}'")
    exit(1)


def ignore_dependency(dependency):
    return (
        dependency.startswith("ta-")
        or dependency.startswith("td-")
        or dependency.startswith("te-")
        or dependency.startswith("tm-")
        or dependency.startswith("ty-")
        or dependency == "tabsdata"
    )


os.makedirs(TARGET_DIR, exist_ok=True)

if os.path.exists(TARGET_FILE):
    os.remove(TARGET_FILE)

cargo_tomls = []
skip = {"node_modules", "target", "tabsdata-ui", "venv"}
for root, dirs, files in os.walk(".", topdown=True, followlinks=True):
    dirs[:] = [d for d in dirs if d not in skip]
    if "Cargo.toml" in files:
        logger.debug(f"🍅 Found manifest at {root}")
        cargo_tomls.append(os.path.join(root, "Cargo.toml"))

aggregated = {}
for cargo_toml in cargo_tomls:
    cargo_output = None
    try:
        logger.debug(f"🍅 Processing manifest: {cargo_toml}")
        result = subprocess.run(
            [
                "cargo",
                "license",
                "--manifest-path",
                cargo_toml,
                "--json",
            ],
            capture_output=True,
            text=True,
            check=True,
            encoding="utf-8",
            errors="strict",
        )
        cargo_output = result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Error running cargo license for manifest {cargo_toml}: {e}")
        exit(1)
    except Exception as e:
        logger.error(
            "❌ Error processing response from cargo license for manifest"
            f" {cargo_toml}: {e}"
        )
        exit(1)

    try:
        packages = json.loads(cargo_output)
    except json.JSONDecodeError as e:
        logger.error(
            "❌ Error parsing json output from cargo license for manifest"
            f" {cargo_toml}: {e}"
        )
        continue

    for pkg in packages:
        name = pkg.get("name", "")
        if not name:
            logger.error(f"❌ Found packages with ho name in manifest {cargo_toml}")
            exit(1)
        if name in aggregated:
            continue
        aggregated[name] = {
            "name": name,
            "version": pkg.get("version", ""),
            "license": pkg.get("license", ""),
        }

licenses_csv_data = [
    f"\"{pkg['name']}\",\"{pkg['version']}\",\"{pkg['license']}\""
    for pkg in aggregated.values()
]
csv_output = "\n".join(licenses_csv_data)

csv_file = io.StringIO(csv_output)
reader = csv.reader(csv_file)
data = list(reader)  #

data = [row for row in data if not ignore_dependency(row[0])]
for row in data:
    dependency_name_tag = row[0]
    dependency_license_tag = row[2]
    custom_license = get_custom_license(dependency_name_tag, dependency_license_tag)
    row[2] = custom_license
data.sort(key=lambda x: (x[2], x[0]))

headers = ["Name", "Version", "License"]
content = (
    "This project includes code from the following Rust crates and versions (grouped by"
    " license):\n\n"
)
content += tabulate(data, headers=headers, tablefmt="fancy_grid")

with open(TARGET_FILE, "w", encoding="utf-8") as f:
    f.write(content + "\n")

logger.debug(content)
