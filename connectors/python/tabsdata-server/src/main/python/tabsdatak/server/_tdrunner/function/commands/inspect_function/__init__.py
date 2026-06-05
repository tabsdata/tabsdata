#
# Copyright 2026 Tabs Data Inc.
#

"""inspect-function command.

Troubleshooting probe that dumps relevant execution information to
stdout so it lands in the pod log for diagnosis. Can be inserted at any
point of a function-run pod chain and any number of times. Intended for
development and diagnosis only; production pods should not include it.
"""

from __future__ import annotations

import argparse
import logging
import os

from tabsdatak.server._tdrunner.function._lib.cli import build_parser, command

logger = logging.getLogger(__name__)

VERSION = "2.0.0"
INFO = "inspect-function — dump function-run execution information for troubleshooting"
MAN = (
    "inspect-function --liveness <PATH>\n\n"
    "Dumps relevant execution information for a function-run pod to "
    "stdout. Can be inserted at any point of the pod chain and any "
    "number of times. Troubleshooting only."
)


def _build_parser():
    return build_parser(
        prog="inspect-function",
        description="Dump function-run execution information for troubleshooting.",
        version=VERSION,
        info=INFO,
        man=MAN,
    )


@command(name="td_inspect_function", parser=_build_parser)
def main(_args: argparse.Namespace) -> int:
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


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
