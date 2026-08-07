#
# Copyright 2026 Tabsdata Inc.
#

"""inspect-function runner.

The command's logic, callable without argparse. The package ``__init__``
wraps :func:`run` in the ``@command`` entrypoint for the console script.
"""

from __future__ import annotations

import argparse
import logging
import os

from tabsdatak.server._tdrunner.function._lib.system_info import report_system

logger = logging.getLogger(__name__)


def run(args: argparse.Namespace) -> int:
    worker = os.environ.get("TD_WORKER", "<unset>")
    decorator = os.environ.get("TD_DECORATOR", "<unset>")
    runid = os.environ.get("TD_RUNID", "<unset>")
    print(
        "--- inspect-function (start) ---",
        flush=True,
    )
    print(
        f"worker={worker}",
        flush=True,
    )
    print(
        f"runid={runid}",
        flush=True,
    )
    print(
        f"decorator={decorator}",
        flush=True,
    )
    report_system()
    print(
        "--- inspect-function (end) ---",
        flush=True,
    )
    return 0
