#
# Copyright 2026 Tabs Data Inc.
#

"""fetch-request command.

Performs `<resolver-method> <resolver-url>` with the given Authorization
header, extracts the ``data`` field from the JSON envelope returned by
the apiserver, and writes it as YAML to `--request-file`. Used to
materialize the function-run request envelope before the rest of the
chain runs.
"""

from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from pathlib import Path

import yaml
from tenacity import (
    retry,
    stop_after_delay,
    wait_fixed,
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

# Command-specific exit codes (>= 10 per docs/01-CommandBasics.md).
EXIT_HTTP_ERROR = 10
EXIT_WRITE_ERROR = 11
EXIT_BAD_PAYLOAD = 12

RESOLVER_RETRY_WAIT_SECONDS = 2
RESOLVER_RETRY_TOTAL_SECONDS = 32


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
        help="Path to write the fetched request body.",
    )
    return parser


@retry(
    stop=stop_after_delay(RESOLVER_RETRY_TOTAL_SECONDS),
    wait=wait_fixed(RESOLVER_RETRY_WAIT_SECONDS),
    reraise=True,
)
def _fetch_request(request: urllib.request.Request) -> bytes:
    with urllib.request.urlopen(request) as response:
        return response.read()


def _fetch(url: str, method: str, authorization: str) -> bytes:
    request = urllib.request.Request(
        url,
        method=method,
        headers={"Authorization": authorization},
    )
    try:
        return _fetch_request(request)
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
    request_file = Path(args.request_file)

    body = _fetch(url, method, authorization)
    try:
        envelope = json.loads(body)
    except json.JSONDecodeError as exc:
        raise CommandSpecificError(
            EXIT_BAD_PAYLOAD, f"response from {url} is not valid JSON: {exc}"
        ) from exc
    if not isinstance(envelope, dict) or "data" not in envelope:
        raise CommandSpecificError(
            EXIT_BAD_PAYLOAD,
            f"response from {url} has no 'data' field",
        )
    data = envelope["data"]
    try:
        request_file.parent.mkdir(parents=True, exist_ok=True)
        request_file.write_text(yaml.safe_dump(data, sort_keys=False))
    except OSError as exc:
        raise CommandSpecificError(
            EXIT_WRITE_ERROR, f"failed to write {request_file}: {exc}"
        ) from exc
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
