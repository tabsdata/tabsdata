#
# Copyright 2026 Tabs Data Inc.
#

"""fetch-request command.

Performs `<resolver-method> <resolver-url>` with the given Authorization
header and writes the response body to `--out`. Used to materialize the
function-run request envelope from the apiserver's resolver endpoint
before the rest of the chain runs.
"""

from __future__ import annotations

import argparse
import urllib.error
import urllib.request
from pathlib import Path

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
)

from tabsdatak.server._tdrunner.function._lib.cli import (
    build_parser,
    command,
    resolve_value,
)
from tabsdatak.server._tdrunner.function._lib.errors import CommandSpecificError

VERSION = "2.0.0"
INFO = (
    "fetch-request — fetch a function-run definition (request.yaml) from the API server"
)
MAN = (
    "fetch-request --resolver-url <URL> --resolver-method <METHOD> "
    "--resolver-authorization <AUTH> --out <PATH> --liveness <PATH>\n\n"
    "Performs the resolver request with the given Authorization header "
    "and writes the response body to --out."
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
        "--out",
        required=True,
        help="Output file path.",
    )
    return parser


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    reraise=True,
)
def _http_request(url: str, method: str, authorization: str) -> bytes:
    req = urllib.request.Request(
        url, method=method, headers={"Authorization": authorization}
    )
    with urllib.request.urlopen(req) as resp:
        return resp.read()


def _fetch(url: str, method: str, authorization: str) -> bytes:
    try:
        return _http_request(url, method, authorization)
    except urllib.error.HTTPError as exc:
        raise CommandSpecificError(
            EXIT_HTTP_ERROR, f"HTTP {exc.code} from {url}: {exc.reason}"
        ) from exc
    except urllib.error.URLError as exc:
        raise CommandSpecificError(
            EXIT_HTTP_ERROR,
            f"network error contacting {url}: {exc.reason}",
        ) from exc


@command(name="td_fetch_request", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    url = resolve_value(args.resolver_url)
    method = resolve_value(args.resolver_method)
    authorization = resolve_value(args.resolver_authorization)
    out_path = Path(args.out)

    body = _fetch(url, method, authorization)
    try:
        out_path.write_bytes(body)
    except OSError as exc:
        raise CommandSpecificError(
            EXIT_WRITE_ERROR, f"failed to write {out_path}: {exc}"
        ) from exc
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
