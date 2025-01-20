#
# Copyright 2025 Tabs Data Inc.
#

import csv
import re
import sys

from tabulate import tabulate


def get_custom_license(dependency_name, dependency_license):
    known_licenses = {
        "Apache",
        "BSD",
        "BSL",
        "CC0",
        "ISC",
        "MIT",
        "MPL",
        "Unicode",
        "Zlib",
    }
    if any(term.lower() in dependency_license.lower() for term in known_licenses):
        if dependency_license == "Apache-2.0) OR MIT AND (MIT":
            return "MIT and (MIT or Apache-2.0)"
        return re.sub(
            r"\b(AND|OR|WITH)\b", lambda m: m.group(1).lower(), dependency_license
        )
    if dependency_name in {
        "dot-generator",
        "dot-structures",
        "graphviz-rust",
        "into-attr",
        "into-attr-derive",
    }:
        return "MIT"
    elif dependency_name == "polars-arrow-format":
        return "Apache 2.0"
    elif dependency_name == "ring":
        return "ICS and OpenSSL (Apache 2.0)"
    else:
        return "Unknown"


def ignore_dependency(dependency):
    return dependency.startswith("td-") or dependency == "tabsdata"


reader = csv.reader(sys.stdin)
data = list(reader)
data = [row for row in data if not ignore_dependency(row[0])]
for row in data:
    dependency_name_tag = row[0]
    dependency_license_tag = row[2]
    custom_license = get_custom_license(dependency_name_tag, dependency_license_tag)
    if custom_license is None:
        pass
    elif custom_license == "Unknown":
        row[2] = f"\nUnknown license for {dependency_name_tag}"
    else:
        row[2] = custom_license
data.sort(key=lambda x: (x[2], x[0]))

headers = ["Name", "Version", "License"]

print(
    "This project includes code from the following Rust crates and versions (grouped by"
    " license):\n"
)
print(tabulate(data, headers=headers, tablefmt="fancy_grid"))
