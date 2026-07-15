#
# Copyright 2026 Tabs Data Inc.
#

"""
Bootstrap entry point for function execution.

This module handles sys.path reordering before any tabsdata imports occur.
It must be the entry point when sys.path ordering is critical for correct
namespace package resolution in editable installations.

This code is intentionally duplicated in modules test_tabsdata/bootest and
test_tabsdatak/bootest as it is trying to fix a wrong sys path order on Windows
that interferes with correct import of namespace packages for editable
installations. Just importing the other implementation wouldn't work as sys
path is broken.
"""

from __future__ import annotations

import logging
import os
import sys

TD_ON_GA: str = "TD_ON_GA"

TRUE_VALUES = {"1", "true", "yes", "y", "on"}

logger = logging.getLogger(__name__)


def order_sys_path():
    on_ga = (
        os.getenv(
            TD_ON_GA,
            "False",
        ).lower()
        in TRUE_VALUES
    )

    if not on_ga:
        logger.debug(
            "Function is being executed on a common setup. "
            "Skipping sys path reordering."
        )
        return

    logger.debug(
        "Function is being executed on a ga setup. Forcing sys path reordering."
    )

    sys_path = "\n".join(sys.path)
    logger.debug(f"Python sys path before reordering: {sys_path}")

    src_main_python_fragment = os.path.join(
        "src",
        "main",
        "python",
    )
    tabsdata_fragment = os.path.join(
        "client",
        "platform",
        "src",
        "main",
        "python",
    )
    tabsdatax_fragment = os.path.join(
        "devutils",
        "automation",
        "tasks",
        "python",
        "tabsdata-toolkit",
        "src",
        "main",
        "python",
    )

    tabsdata_sys_paths = []
    non_tabsdata_sys_paths = []

    for path in sys.path:
        if path.endswith(src_main_python_fragment):
            tabsdata_sys_paths.append(path)
        else:
            non_tabsdata_sys_paths.append(path)

    tabsdata_paths = []
    other_paths = []

    for path in tabsdata_sys_paths:
        if path.endswith(tabsdatax_fragment):
            tabsdata_paths.append(path)

    for path in tabsdata_sys_paths:
        if path.endswith(tabsdata_fragment):
            tabsdata_paths.append(path)

    for path in tabsdata_sys_paths:
        if path.endswith(tabsdata_fragment) or path.endswith(tabsdatax_fragment):
            pass
        else:
            other_paths.append(path)

    other_paths.sort()

    ordered_tabsdata_paths = list(tabsdata_paths)
    ordered_tabsdata_paths.extend(other_paths)
    sys.path[:] = non_tabsdata_sys_paths + ordered_tabsdata_paths

    sys_path = "\n".join(sys.path)
    logger.debug(f"Python sys path after reordering: {sys_path}")


order_sys_path()

from tabsdata._tabsserver.function.execute_function_from_bundle_path import (  # noqa: E402
    main,
)

if __name__ == "__main__":
    main()
