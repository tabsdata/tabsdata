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
import logging
import urllib.error
import urllib.request
from pathlib import Path

import yaml
from tenacity import (
    retry,
    stop_after_delay,
    wait_fixed,
)

from tabsdatak.server._tdrunner.function._lib.command import (
    build_parser,
    command,
    resolve_value,
)
from tabsdatak.server._tdrunner.function.commands.fetch_request.error import (
    FetchRequestErrorCode,
)

logger = logging.getLogger(__name__)

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
        # Surface the apiserver's response body in the detail so a 4xx
        # carrying a JSON error description (auth scope mismatch, bad
        # bindings, ...) diagnoses itself instead of bottoming out at a
        # bare "Not Found" / "Bad Request".
        body = exc.read().decode("utf-8", errors="replace")
        detail = f"{exc.reason}: {body}" if body else exc.reason
        raise FetchRequestErrorCode.FR_1.exception(
            cause=exc, url=url, status=exc.code, detail=detail
        )
    except urllib.error.URLError as exc:
        raise FetchRequestErrorCode.FR_2.exception(
            cause=exc, url=url, detail=str(exc.reason)
        )


@command(name="td_fetch_request", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    url = resolve_value(args.resolver_url)
    method = resolve_value(args.resolver_method)
    authorization = resolve_value(args.resolver_authorization)
    request_file = Path(args.request_file)
    logger.info("Fetching function-run request from the apiserver")
    logger.debug("Resolver: %s %s -> %s", method, url, request_file)

    body = _fetch(url, method, authorization)
    logger.debug("Received %d bytes from resolver", len(body))
    try:
        envelope = json.loads(body)
    except json.JSONDecodeError as exc:
        raise FetchRequestErrorCode.FR_4.exception(
            cause=exc, url=url, expected="JSON", detail=str(exc)
        )
    if not isinstance(envelope, dict) or "data" not in envelope:
        raise FetchRequestErrorCode.FR_4.exception(
            url=url, expected="JSON", detail="missing 'data' field"
        )
    data = envelope["data"]
    try:
        request_file.parent.mkdir(parents=True, exist_ok=True)
        request_file.write_text(yaml.safe_dump(data, sort_keys=False))
    except OSError as exc:
        # TD-2270 introduced this typo (`out_path` ↦ `request_file`);
        # fixed during the rebase resolution.
        raise FetchRequestErrorCode.FR_3.exception(
            cause=exc, path=request_file, detail=str(exc)
        )
    logger.info("Wrote request.yaml to %s", request_file)
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
