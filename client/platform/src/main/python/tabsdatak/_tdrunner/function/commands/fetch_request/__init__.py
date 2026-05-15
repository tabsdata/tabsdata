#
# Copyright 2026 Tabs Data Inc.
#

"""fetch-request command.

Step 1 of the publisher init-container chain. Performs `GET <url>` with
`Authorization: Bearer <token>` and writes the response body to `--out`.
See `docs/02-PublisherInitContainers.md` for the chain context.
"""

from __future__ import annotations

import urllib.error
import urllib.request
from pathlib import Path

from tabsdatak._tdrunner.function._lib.cli import (
    build_parser,
    resolve_value,
    run_command,
)
from tabsdatak._tdrunner.function._lib.errors import CommandSpecificError
from tabsdatak._tdrunner.function._lib.liveness import Liveness
from tabsdatak._tdrunner.function.commands import tabsdata_command

VERSION = "0.1.0"
INFO = (
    "fetch-request — fetch a function-run definition (request.yaml) from the API server"
)
MAN = (
    "fetch-request --url <URL> --token <TOKEN> --out <PATH> "
    "--liveness <PATH>\n\n"
    "Performs a GET on --url with `Authorization: Bearer <token>` and "
    "writes the response body to --out. Used as the first step of the "
    "function-run pod chain."
)

# Command-specific exit codes (>= 10 per docs/01-CommandBasics.md).
EXIT_HTTP_ERROR = 10
EXIT_WRITE_ERROR = 11


def _build_parser():
    parser = build_parser(
        prog="fetch-request",
        description="Fetch a function-run definition (request.yaml) from a URL.",
        version=VERSION,
        info=INFO,
        man=MAN,
    )
    parser.add_argument("--url", required=True, help="URL or env:VAR / val:VALUE")
    parser.add_argument(
        "--token", required=True, help="Bearer token or env:VAR / val:VALUE"
    )
    parser.add_argument("--out", required=True, help="Output file path")
    return parser


def _run() -> int:
    args = _build_parser().parse_args()
    url = resolve_value(args.url)
    token = resolve_value(args.token)
    out_path = Path(args.out)
    liveness_path = Path(args.liveness)

    with Liveness(liveness_path):
        body = _fetch(url, token)
        try:
            out_path.write_bytes(body)
        except OSError as exc:
            raise CommandSpecificError(
                EXIT_WRITE_ERROR, f"failed to write {out_path}: {exc}"
            ) from exc
    return 0


def _fetch(url: str, token: str) -> bytes:
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.read()
    except urllib.error.HTTPError as exc:
        raise CommandSpecificError(
            EXIT_HTTP_ERROR, f"HTTP {exc.code} from {url}: {exc.reason}"
        ) from exc
    except urllib.error.URLError as exc:
        raise CommandSpecificError(
            EXIT_HTTP_ERROR,
            f"network error contacting {url}: {exc.reason}",
        ) from exc


@tabsdata_command(name="td_fetch_request")
def main() -> int:
    return run_command(_run)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
