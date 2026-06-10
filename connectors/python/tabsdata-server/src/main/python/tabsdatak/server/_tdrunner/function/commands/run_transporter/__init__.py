#
# Copyright 2026 Tabs Data Inc.
#

"""run-transporter command.

Steps 4b and 7b of the publisher init-container chain. Thin wrapper that
exec's `td_transporter --request <PATH>` while running a liveness ticker.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from tabsdatak.server._tdrunner.function._lib.command import build_parser, command
from tabsdatak.server._tdrunner.function._lib.transporter_proc import (
    TransporterNotFoundError,
    run_transporter,
)
from tabsdatak.server._tdrunner.function.commands.run_transporter.error import (
    RunTransporterErrorCode,
)

logger = logging.getLogger(__name__)

VERSION = "0.1.0"
INFO = "run-transporter — exec td_transporter with the given request YAML"
MAN = (
    "run-transporter --request <PATH> --report <PATH> --liveness <PATH>\n\n"
    "Exec's `td_transporter --request <PATH> --report <PATH>` with "
    "stdout/stderr inherited; returns whatever exit code the binary "
    "returned. The report file must not already exist; the binary "
    "writes its execution report there."
)


def _build_parser():
    parser = build_parser(
        prog="run-transporter",
        description="Exec td_transporter with a transporter request YAML.",
        version=VERSION,
        info=INFO,
        man=MAN,
    )
    parser.add_argument(
        "--request",
        required=True,
        help="Path to the transporter request YAML",
    )
    parser.add_argument(
        "--report",
        required=True,
        help=(
            "Path to write the transporter execution report. Must not already exist."
        ),
    )
    return parser


@command(name="td_run_transporter", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    request_yaml = Path(args.request)
    report = Path(args.report)
    logger.info("Running td_transporter")
    logger.debug("Request: %s  Report: %s", request_yaml, report)
    try:
        rc = run_transporter(request_yaml, report)
    except TransporterNotFoundError as exc:
        raise RunTransporterErrorCode.RUN_TR_1.exception(cause=exc, detail=str(exc))
    if rc == 0:
        logger.info("td_transporter completed successfully")
    else:
        logger.warning("td_transporter exited with non-zero code %d", rc)
    return rc


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
