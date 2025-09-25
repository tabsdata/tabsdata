#
# Copyright 2025 Tabs Data Inc.
#

import csv
import importlib
import importlib.util
import os
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

LICENSES_RS_TXT = "./target/audit/licenses_rs.txt"
LICENSES_PY_TXT = "./target/audit/licenses_py.txt"
LICENSES_PY_CSV = "./target/audit/licenses_py.csv"
LICENSES_AG_PY_TXT = "./target/audit/licenses_ag_py.txt"
LICENSES_AG_PY_CSV = "./target/audit/licenses_ag_py.csv"
LICENSES_TS_TXT = "./target/audit/licenses_ts.txt"

LICENSES_FILE = "./target/audit/licenses.txt"
THIRD_PARTY_FILE = "./variant/assets/manifest/THIRD-PARTY"


def read_file(filepath):
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as file:
            return file.read().strip()
    return ""


def merge_files():
    all_data = []
    for csv_file in [LICENSES_PY_CSV, LICENSES_AG_PY_CSV]:
        if os.path.exists(csv_file):
            with open(csv_file, "r", encoding="utf-8") as csvf:
                reader = csv.reader(csvf)
                all_data.extend(list(reader))

    if not all_data:
        return ""

    seen_data = set()
    unique_data = []
    for row in all_data:
        if len(row) >= 2:
            key = (row[0], row[1])
            if key not in seen_data:
                seen_data.add(key)
                unique_data.append(row)

    unique_data.sort(key=lambda package: (package[2], package[0], package[1]))

    if unique_data:
        txt_headers = ["Name", "Version", "License"]
        txt_content = (
            "This project uses the following Python packages and versions (grouped by"
            " license):\n\n"
        )
        txt_content += tabulate(unique_data, headers=txt_headers, tablefmt="fancy_grid")
        return txt_content + "\n"

    return ""


content_rs = read_file(LICENSES_RS_TXT)
content_py = merge_files()
content = f"{content_rs}\n\n{content_py}\n"
if os.path.exists(LICENSES_TS_TXT):
    content_ts = read_file(LICENSES_TS_TXT)
    content += f"\n{content_ts}\n"

for output in [LICENSES_FILE, THIRD_PARTY_FILE]:
    os.makedirs(os.path.dirname(output), exist_ok=True)
    with open(output, "w", encoding="utf-8") as f:
        f.write(content)
