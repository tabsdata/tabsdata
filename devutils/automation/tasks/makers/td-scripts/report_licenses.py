#
# Copyright 2025 Tabs Data Inc.
#

import importlib
import importlib.util
import os
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

LICENSES_RS = "./target/audit/licenses_rs.txt"
LICENSES_PY = "./target/audit/licenses_py.txt"

LICENSES_FILE = "./target/audit/licenses.txt"
THIRD_PARTY_FILE = "./variant/assets/manifest/THIRD-PARTY"


def read_file(filepath):
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as file:
            return file.read().strip()
    return ""


content_rs = read_file(LICENSES_RS)
content_py = read_file(LICENSES_PY)
content = f"{content_rs}\n\n{content_py}\n"

for output in [LICENSES_FILE, THIRD_PARTY_FILE]:
    os.makedirs(os.path.dirname(output), exist_ok=True)
    with open(output, "w", encoding="utf-8") as f:
        f.write(content)
