#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import yaml

from tabsdatak.server._tdrunner.function._commons.http.request import (
    DEFAULT_TIMEOUT_SECONDS,
)
from tabsdatak.server._tdrunner.function._commons.http.retry import call_with_retry
from tabsdatak.server._tdrunner.function._commons.run.command import Command
from tabsdatak.server._tdrunner.function._commons.solver.arguments import resolve_value
from tabsdatak.server._tdrunner.function.commands.gathering.fetch_request.error import (
    FetchRequestErrorCode,
)
from tabsdatak.server._tdrunner.function.commands.gathering.fetch_request.options import (  # noqa: E501
    FetchRequestArguments,
)

logger = logging.getLogger(__name__)

ENVELOPE_DATA_KEY = "data"


class FetchRequest(Command[FetchRequestArguments]):
    url: str
    method: str
    authorization: str
    request_file: Path
    data: Any

    def __init__(self, arguments: FetchRequestArguments) -> None:
        super().__init__(arguments)
        self.url = resolve_value(arguments.resolver_url)
        self.method = resolve_value(arguments.resolver_method)
        self.authorization = resolve_value(arguments.resolver_authorization)
        self.request_file = arguments.request_file

    def run(self) -> int:
        logger.info("Fetching the function-run request from the apiserver")
        logger.debug(f"Resolver {self.method} {self.url} goes to {self.request_file}")
        self.data = self.unwrap(self.fetch())
        self.write()
        logger.info(f"Wrote the request to {self.request_file}")
        return 0

    def fetch(self) -> bytes:
        request = urllib.request.Request(
            self.url,
            method=self.method,
            headers={"Authorization": self.authorization},
        )
        try:
            answer = call_with_retry(self.send, request)
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")
            if body:
                detail = f"{error.reason}: {body}"
            else:
                detail = error.reason
            raise FetchRequestErrorCode.FR_1.exception(
                cause=error,
                url=self.url,
                status=error.code,
                detail=detail,
            )
        except urllib.error.URLError as error:
            raise FetchRequestErrorCode.FR_2.exception(
                cause=error,
                url=self.url,
                detail=str(error.reason),
            )
        logger.debug(f"Received {len(answer)} bytes from the resolver")
        return answer

    @staticmethod
    def send(request: urllib.request.Request) -> bytes:
        with urllib.request.urlopen(
            request, timeout=DEFAULT_TIMEOUT_SECONDS
        ) as response:
            return response.read()

    def unwrap(self, answer: bytes) -> Any:
        try:
            envelope = json.loads(answer)
        except json.JSONDecodeError as error:
            raise FetchRequestErrorCode.FR_4.exception(
                cause=error,
                url=self.url,
                expected="JSON",
                detail=str(error),
            )
        if not isinstance(envelope, dict) or ENVELOPE_DATA_KEY not in envelope:
            raise FetchRequestErrorCode.FR_4.exception(
                url=self.url,
                expected="JSON",
                detail=f"missing '{ENVELOPE_DATA_KEY}' field",
            )
        return envelope[ENVELOPE_DATA_KEY]

    def write(self) -> None:
        try:
            self.request_file.parent.mkdir(parents=True, exist_ok=True)
            self.request_file.write_text(yaml.safe_dump(self.data, sort_keys=False))
        except OSError as error:
            raise FetchRequestErrorCode.FR_3.exception(
                cause=error,
                path=self.request_file,
                detail=str(error),
            )
