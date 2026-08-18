#
# Copyright 2026 Tabsdata Inc.
#

"""inspect-function command.

Troubleshooting probe that dumps relevant execution information to
stdout so it lands in the pod log for diagnosis. Can be inserted at any
point of a function-run pod chain and any number of times. Intended for
development and diagnosis only; production pods should not include it.
"""

from __future__ import annotations

import argparse

from tabsdatak.server._tdrunner.function._lib.command import build_parser, command
from tabsdatak.server._tdrunner.function.commands.inspect_function.runner import run

__all__ = ["run", "main"]

VERSION = "2.0.1"
INFO = "inspect-function — dump function-run execution information for troubleshooting"
MAN = (
    "inspect-function\n"
    "    --shared-dir <PATH>\n"
    "    --work-dir <PATH>\n"
    "    --log-config <PATH>\n"
    "    --liveness <PATH>\n"
    "    --responder-url <URL>\n"
    "    --responder-method <METHOD>\n"
    "    --responder-authorization <AUTH>\n\n"
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
def main(args: argparse.Namespace) -> int:
    return run(args)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
