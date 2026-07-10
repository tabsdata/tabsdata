#
# Copyright 2026 Tabs Data Inc.
#

"""inspect-function runner.

The command's logic, callable without argparse. The package ``__init__``
wraps :func:`run` in the ``@command`` entrypoint for the console script.
"""

from __future__ import annotations

import argparse
import logging
import os

logger = logging.getLogger(__name__)


def run(args: argparse.Namespace) -> int:
    worker = os.environ.get("TD_WORKER", "<unset>")
    decorator = os.environ.get("TD_DECORATOR", "<unset>")
    runid = os.environ.get("TD_RUNID", "<unset>")
    print(
        f"--- inspect-function: worker={worker} runid={runid} "
        f"decorator={decorator} ---",
        flush=True,
    )
    for key in sorted(os.environ):
        print(f"  {key}={os.environ[key]}", flush=True)
    print("--- inspect-function: done ---", flush=True)
    return 0
