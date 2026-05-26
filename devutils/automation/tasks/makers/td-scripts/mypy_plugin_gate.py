#
# Copyright 2026 Tabs Data Inc.
#

import importlib
import importlib.util
import os
import sys
from types import ModuleType

from mypy import errorcodes


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


def builtin_error_codes() -> list[str]:
    return sorted(
        value.code
        for value in vars(errorcodes).values()
        if isinstance(value, errorcodes.ErrorCode)
    )


def main() -> None:
    for code in builtin_error_codes():
        sys.stdout.write("--disable-error-code\n")
        sys.stdout.write(code)
        sys.stdout.write("\n")


main()
