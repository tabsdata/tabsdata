#
# Copyright 2025 Tabs Data Inc.
#

import json
import sys

from tabulate import tabulate

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


def ignore_package(package):
    return package.startswith("td-") or package == "tabsdata"


reader = sys.stdin.read()
data = json.loads(reader)
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
print(
    "\nThis project uses the following Python packages and versions (grouped by"
    " license):\n"
)
print(tabulate(table_data, headers=headers, tablefmt="fancy_grid"))
