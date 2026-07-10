#
# Copyright 2026 Tabs Data Inc.
#

"""fetch-request command.

Performs `<resolver-method> <resolver-url>` with the given Authorization
header, extracts the ``data`` field from the JSON envelope returned by
the apiserver, and writes it as YAML to `--request-file`. Used to
materialize the function-run request envelope before the rest of the
chain runs.

The logic lives in :mod:`.runner`; this module is the thin ``@command``
entrypoint the console script resolves to. The compound imports
:func:`.runner.run` directly instead of going through ``main``.
"""

from __future__ import annotations

import argparse

from tabsdatak.server._tdrunner.function._lib.command import build_parser, command
from tabsdatak.server._tdrunner.function.commands.fetch_request.runner import (
    _fetch,
    run,
)

__all__ = ["_fetch", "run", "main"]

VERSION = "2.0.0"
INFO = (
    "fetch-request — fetch a function-run definition (request.yaml) from the API server"
)
MAN = (
    "fetch-request\n"
    "    --shared-dir <PATH>\n"
    "    --work-dir <PATH>\n"
    "    --log-config <PATH>\n"
    "    --liveness <PATH>\n"
    "    --responder-url <URL>\n"
    "    --responder-method <METHOD>\n"
    "    --responder-authorization <AUTH>\n"
    "    --resolver-url <URL>\n"
    "    --resolver-method <METHOD>\n"
    "    --resolver-authorization <AUTH>\n"
    "    --request-file <PATH>\n\n"
    "Performs the resolver request with the given Authorization header, "
    "extracts the 'data' field from the JSON envelope, and writes it as "
    "YAML to --request-file."
)


def _build_parser():
    parser = build_parser(
        prog="fetch-request",
        description="Fetch a function-run definition (request.yaml) from a URL.",
        version=VERSION,
        info=INFO,
        man=MAN,
    )
    parser.add_argument(
        "--resolver-url",
        required=True,
        help="Resolver endpoint URL.",
    )
    parser.add_argument(
        "--resolver-method",
        required=True,
        help="Resolver HTTP method.",
    )
    parser.add_argument(
        "--resolver-authorization",
        required=True,
        help="Resolver Authorization header value.",
    )
    parser.add_argument(
        "--request-file",
        required=True,
        help="Path to write the fetched request run.",
    )
    return parser


@command(name="td_fetch_request", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    return run(args)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
