#
# Copyright 2025 Tabs Data Inc.
#

import csv
import importlib
import importlib.util
import io
import json
import os
import re
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


def get_custom_license(dependency_name, dependency_license):
    known_licenses = {
        "Apache",
        "BSD",
        "BSL",
        "CC0",
        "CDLA",
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


os.makedirs(TARGET_DIR, exist_ok=True)

if os.path.exists(TARGET_FILE):
    os.remove(TARGET_FILE)

try:
    result = subprocess.run(
        ["cargo", "license", "--manifest-path", "Cargo.toml", "--json"],
        capture_output=True,
        text=True,
        check=True,
    )
    cargo_output = result.stdout
except subprocess.CalledProcessError as e:
    logger.error(f"❌ Error running cargo license: {e}")
    exit(1)
except Exception as e:
    logger.error(f"❌ Error processing response from cargo license: {e}")
    exit(1)

try:
    licenses_json_data = json.loads(cargo_output)
    licenses_csv_data = [
        f'"{pkg["name"]}","{pkg["version"]}","{pkg["license"]}"'
        for pkg in licenses_json_data
    ]
    csv_output = "\n".join(licenses_csv_data)
except json.JSONDecodeError as e:
    logger.error(f"❌ Error parsing json output from cargo license: {e}")
    exit(1)


csv_file = io.StringIO(csv_output)
reader = csv.reader(csv_file)
data = list(reader)  #

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
content = (
    "This project includes code from the following Rust crates and versions (grouped by"
    " license):\n\n"
)
content += tabulate(data, headers=headers, tablefmt="fancy_grid")

with open(TARGET_FILE, "w", encoding="utf-8") as f:
    f.write(content + "\n")

logger.debug(content)
