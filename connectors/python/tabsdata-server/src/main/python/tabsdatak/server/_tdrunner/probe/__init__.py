#
# Copyright 2026 Tabsdata Inc.
#

"""probe command.

The single step of a connection-probe pod: loads the connection mounted from
its ConfigMap, with the secret values the pod receives as ``SECRET_CONN__*``
environment variables, runs the requested Explorer call and posts the outcome
to the responder. See :mod:`.runner` for the request and outcome shapes.
"""

from __future__ import annotations

import argparse

from tabsdatak.server._tdrunner.function._commons.run.command import (
    build_parser,
    command,
)
from tabsdatak.server._tdrunner.probe.runner import run

__all__ = ["run", "main"]

VERSION = "2.2.0"
INFO = "probe — run one connector Explorer call against a connection"
MAN = (
    "probe --request <PATH> --connection-file <PATH>\n"
    "    --liveness <PATH> --journal-file <PATH>\n"
    "    --responder-url <URL> --responder-method <METHOD>\n"
    "    --responder-authorization <AUTH>\n\n"
    "Reads the probe request JSON, runs the check, metadata or sample call "
    "of the connection's Explorer under the kind's timeout and posts the "
    "outcome to the responder as the Done callback's output."
)


def _build_parser():
    parser = build_parser(
        program="probe",
        description="Run one connector Explorer call against a connection.",
        version=VERSION,
        info=INFO,
        manual=MAN,
    )
    parser.add_argument("--request", required=True, help="Path to request.json")
    parser.add_argument(
        "--connection-file",
        required=True,
        dest="connection_file",
        help="Path to the connection definition file.",
    )
    return parser


@command(name="td_probe", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    return run(args)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
