#
# Copyright 2025 Tabs Data Inc.
#

import importlib
import importlib.util
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

TARGET_DIR = os.path.join("..", "target", "audit")
TARGET_FILE = os.path.join(TARGET_DIR, "licenses_py.txt")

# fmt: off
normalized_licenses = {
    "": "Unspecified License",                                                                                                                                                                 # noqa: E231,E241,E501
    "(Apache-2.0 OR ISC) AND ISC AND OpenSSL": "Apache License Version 2.0 or Internet Systems Consortium License, and Internet Systems Consortium License, and OpenSSL License",              # noqa: E231,E241,E501
    "(Apache-2.0 OR ISC) AND ISC": "Apache License Version 2.0 or Internet Systems Consortium License, and Internet Systems Consortium License",                                               # noqa: E231,E241,E501
    "(Apache-2.0 OR MIT) AND BSD-3-Clause": "Apache License Version 2.0 or MIT License, and BSD Revised License (BSD-3-Clause)",                                                               # noqa: E231,E241,E501
    "(Apache-2.0 OR MIT) AND Unicode-3.0": "Apache License Version 2.0 or MIT License, and Unicode License Version 3.0",                                                                       # noqa: E231,E241,E501
    "(MIT OR CC0-1.0)": "MIT License and Creative Commons Zero Version 1.0 Universal",                                                                                                         # noqa: E231,E241,E501
    "0BSD OR Apache-2.0 OR MIT": "BSD Zero-Clause License or Apache License Version 2.0 or MIT License",                                                                                       # noqa: E231,E241,E501
    "0BSD": "BSD Zero-Clause License",                                                                                                                                                         # noqa: E231,E241,E501
    "APACHE LICENSE 2.0": "Apache License Version 2.0",                                                                                                                                        # noqa: E231,E241,E501
    "APACHE SOFTWARE LICENSE;; BSD LICENSE": "Apache License Version 2.0 or BSD License",                                                                                                      # noqa: E231,E241,E501
    "APACHE SOFTWARE LICENSE;; MIT LICENSE": "Apache License Version 2.0 or MIT License",                                                                                                      # noqa: E231,E241,E501
    "APACHE SOFTWARE LICENSE": "Apache License Version 2.0",                                                                                                                                   # noqa: E231,E241,E501
    "Apache-2.0 AND ISC": "Apache License Version 2.0 and Internet Systems Consortium License",                                                                                                # noqa: E231,E241,E501
    "Apache-2.0 AND MIT": "Apache License Version 2.0 and MIT License",                                                                                                                        # noqa: E231,E241,E501
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
    "APACHE-2.0;; BSD-3-CLAUSE": "Apache License Version 2.0 or BSD Revised License (BSD-3-Clause)",                                                                                           # noqa: E231,E241,E501
    "Apache-2.0": "Apache License Version 2.0",                                                                                                                                                # noqa: E231,E241,E501
    "APACHE-2.0": "Apache License Version 2.0",                                                                                                                                                # noqa: E231,E241,E501
    "Artistic-2.0 OR CC0-1.0": "Artistic License Version 2.0 or Creative Commons Zero Version 1.0 Universal",                                                                                  # noqa: E231,E241,E501
    "BlueOak-1.0.0": "Blue Oak Model License Version 1.0.0",                                                                                                                                   # noqa: E231,E241,E501
    "BSD LICENSE;; APACHE SOFTWARE LICENSE": "BSD Revised License (BSD-3-Clause) or Apache Software License Version 2.0",                                                                      # noqa: E231,E241,E501
    "BSD LICENSE": "BSD License",                                                                                                                                                              # noqa: E231,E241,E501
    "BSD-2-Clause": "BSD Simplified License (BSD-2-Clause)",                                                                                                                                   # noqa: E231,E241,E501
    "BSD-3-Clause AND MIT": "BSD Revised License (BSD-3-Clause), and MIT License",                                                                                                             # noqa: E231,E241,E501
    "BSD-3-Clause OR MIT": "BSD Revised License (BSD-3-Clause) or MIT License",                                                                                                                # noqa: E231,E241,E501
    "BSD-3-Clause": "BSD Revised License (BSD-3-Clause)",                                                                                                                                      # noqa: E231,E241,E501
    "BSD-3-CLAUSE": "BSD Revised License (BSD-3-Clause)",                                                                                                                                      # noqa: E231,E241,E501
    "BSL-1.0": "Boost Software License Version 1.0",                                                                                                                                           # noqa: E231,E241,E501
    "CC-BY-4.0": "Creative Commons Attribution Version 4.0 International License",                                                                                                             # noqa: E231,E241,E501
    "CC0-1.0": "Creative Commons Zero Version 1.0 Universal",                                                                                                                                  # noqa: E231,E241,E501
    "CDLA-Permissive-2.0": "Community Data License Agreement Permissive Version 2.0",                                                                                                          # noqa: E231,E241,E501
    "CMU LICENSE (MIT-CMU)": "MIT License (CMU Variant)",                                                                                                                                      # noqa: E231,E241,E501
    "ISC LICENSE _ISCL_": "Internet Systems Consortium License",                                                                                                                               # noqa: E231,E241,E501
    "ISC": "Internet Systems Consortium License",                                                                                                                                              # noqa: E231,E241,E501
    "MIT LICENSE;; APACHE SOFTWARE LICENSE": "MIT License or Apache Software License Version 2.0",                                                                                             # noqa: E231,E241,E501
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
    "UNLICENSED": "The Unlicense",                                                                                                                                                             # noqa: E231,E241,E501
    "Zlib": "Zlib License",                                                                                                                                                                    # noqa: E231,E241,E501
}
# fmt: on


def get_custom_license(_p_name, p_license):
    key = p_license.strip()
    if key in normalized_licenses:
        return normalized_licenses[key]
    logger.error(f"No label for package license '{p_license}'")
    exit(1)


def ignore_package(name):
    return name.startswith("td-") or name == "tabsdata"


os.makedirs(TARGET_DIR, exist_ok=True)

if os.path.exists(TARGET_FILE):
    os.remove(TARGET_FILE)

try:
    result = subprocess.run(
        ["licensecheck", "-r", "requirements.txt", "--format", "json"],
        capture_output=True,
        text=True,
        check=True,
        encoding="utf-8",
        errors="strict",
    )
    data = json.loads(result.stdout)
except subprocess.CalledProcessError as e:
    logger.error(f"❌ Error running licensecheck: {e}")
    exit(1)
except Exception as e:
    logger.error(f"❌ Error parsing json output from licensecheck: {e}")
    exit(1)

packages = data.get("packages", [])
table_data = []
for package in packages:
    package_name_tag = package.get("name")
    package_version_tag = package.get("version")
    package_license_tag = package.get("license")
    if ignore_package(package_name_tag):
        continue
    custom_license = get_custom_license(package_name_tag, package_license_tag)
    table_data.append([package_name_tag, package_version_tag, custom_license])
table_data.sort(key=lambda x: (x[2], x[0]))

headers = ["Name", "Version", "License"]
content = (
    "This project uses the following Python packages and versions (grouped by"
    " license):\n\n"
)
content += tabulate(table_data, headers=headers, tablefmt="fancy_grid")

with open(TARGET_FILE, "w", encoding="utf-8") as f:
    f.write(content + "\n")

logger.debug(content)
