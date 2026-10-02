#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import urllib.error
from http import HTTPStatus
from typing import Any, Callable, TypeVar

from tenacity import Retrying, retry_if_exception, stop_after_delay, wait_fixed

WAIT_SECONDS = 2
TOTAL_SECONDS = 120

RETRIABLE_CLIENT_STATUSES = frozenset({
    HTTPStatus.REQUEST_TIMEOUT,
    HTTPStatus.TOO_MANY_REQUESTS,
})

T = TypeVar("T")


def is_retriable(error: BaseException) -> bool:
    if isinstance(error, urllib.error.HTTPError):
        return (
            error.code >= HTTPStatus.INTERNAL_SERVER_ERROR
            or error.code in RETRIABLE_CLIENT_STATUSES
        )
    return not isinstance(error, ValueError)


def call_with_retry(call: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    retrying: Retrying = type.__call__(
        Retrying,
        retry=type.__call__(retry_if_exception, predicate=is_retriable),
        stop=type.__call__(stop_after_delay, max_delay=TOTAL_SECONDS),
        wait=type.__call__(wait_fixed, wait=WAIT_SECONDS),
        reraise=True,
    )
    return retrying(call, *args, **kwargs)
