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

TARGET_DIR = os.path.join(".", "target", "audit")
TARGET_FILE = os.path.join(TARGET_DIR, "licenses_py.txt")

normalized_licenses = {
    "APACHE SOFTWARE LICENSE": "Apache Software License",
    "BSD LICENSE": "BSD License",
    "BSD LICENSE;; APACHE SOFTWARE LICENSE": "BSD License and Apache Software License",
    "CMU LICENSE (MIT-CMU)": "CMU License (MIT-CMU)",
    "MIT LICENSE": "MIT License",
    "MIT LICENSE;; APACHE SOFTWARE LICENSE": "MIT License and Apache Software License",
    "MOZILLA PUBLIC LICENSE 2.0 (MPL 2.0)": "Mozilla Public License 2.0 (MPL 2.0)",
    "PYTHON SOFTWARE FOUNDATION LICENSE": "Python Software Foundation License",
    "THE UNLICENSE (UNLICENSE)": "The Unlicense (Unlicense)",
}


def get_custom_license(package_name, package_license):
    known_licenses = {
        "Apache",
        "BSD",
        "MIT",
        "MPL",
        "Python Software Foundation License",
        "Unlicense",
    }
    package_license = normalized_licenses.get(
        package_license.strip(), package_license.strip()
    )
    if any(term.lower() in package_license.lower() for term in known_licenses):
        return package_license
    if package_name == "uuid-v7":
        return "Public Domain"
    elif package_name == "ring":
        return "ICS and OpenSSL (Apache 2.0)"
    else:
        return "Unknown"


def ignore_package(name):
    return name.startswith("td-") or name == "tabsdata"


os.makedirs(TARGET_DIR, exist_ok=True)

if os.path.exists(TARGET_FILE):
    os.remove(TARGET_FILE)

try:
    result = subprocess.run(
        ["licensecheck", "-u", "requirements:requirements.txt", "--format", "json"],
        capture_output=True,
        text=True,
        check=True,
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
    package_name_tag = package.get("name", "UNKNOWN")
    package_version_tag = package.get("version", "UNKNOWN")
    package_license_tag = package.get("license", "UNKNOWN")
    if ignore_package(package_name_tag):
        continue
    custom_license = get_custom_license(package_name_tag, package_license_tag)
    if custom_license is None:
        table_data.append([package_name_tag, package_version_tag, package_license_tag])
    elif custom_license == "Unknown":
        table_data.append(
            [
                package_name_tag,
                package_version_tag,
                "Unknown License",
            ]
        )
    else:
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
