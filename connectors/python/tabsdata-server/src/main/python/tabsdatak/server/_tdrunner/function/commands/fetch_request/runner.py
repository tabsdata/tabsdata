#
# Copyright 2026 Tabs Data Inc.
#

"""fetch-request runner.

The command's logic, callable without argparse or the responder. The
package ``__init__`` wraps :func:`run` in the ``@command`` entrypoint
for the console script; the compound imports :func:`run` directly and
supplies a pre-built ``Namespace``.
"""

from __future__ import annotations

import argparse
import json
import logging
import urllib.error
import urllib.request
from pathlib import Path

import yaml
from tenacity import retry, stop_after_delay, wait_fixed

from tabsdatak.server._tdrunner.function._lib.command import resolve_value
from tabsdatak.server._tdrunner.function.commands.fetch_request.error import (
    FetchRequestErrorCode,
)

logger = logging.getLogger(__name__)

RESOLVER_RETRY_WAIT_SECONDS = 2
RESOLVER_RETRY_TOTAL_SECONDS = 32


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


def run(args: argparse.Namespace) -> int:
    url = resolve_value(args.resolver_url)
    method = resolve_value(args.resolver_method)
    authorization = resolve_value(args.resolver_authorization)
    request_file = Path(args.request_file)
    logger.info("Fetching function-run request from the apiserver")
    logger.debug("Resolver: %s %s -> %s", method, url, request_file)

    fetched = _fetch(url, method, authorization)
    logger.debug("Received %d bytes from resolver", len(fetched))
    try:
        envelope = json.loads(fetched)
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
        raise FetchRequestErrorCode.FR_3.exception(
            cause=exc, path=request_file, detail=str(exc)
        )
    logger.info("Wrote request.yaml to %s", request_file)
    return 0
