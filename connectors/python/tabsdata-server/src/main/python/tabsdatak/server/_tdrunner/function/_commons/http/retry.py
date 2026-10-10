#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import urllib.error
from http import HTTPStatus
from typing import Any, Callable, TypeVar

from tenacity import (
    RetryCallState,
    Retrying,
    retry_any,
    retry_if_exception,
    retry_if_result,
    stop_after_delay,
    stop_any,
    wait_fixed,
)

from tabsdatak.server._tdrunner.function._commons.http.request import HttpResponse
from tabsdatak.server._tdrunner.function._commons.run.shutdown import (
    is_shutdown_requested,
)

WAIT_SECONDS = 2
TOTAL_SECONDS = 120

RETRIABLE_CLIENT_STATUSES = frozenset({
    HTTPStatus.REQUEST_TIMEOUT,
    HTTPStatus.TOO_MANY_REQUESTS,
})

T = TypeVar("T")

logger = logging.getLogger(__name__)


def is_retriable_status(status: int) -> bool:
    return (
        status >= HTTPStatus.INTERNAL_SERVER_ERROR
        or status in RETRIABLE_CLIENT_STATUSES
    )


def is_retriable(error: BaseException) -> bool:
    if isinstance(error, urllib.error.HTTPError):
        return is_retriable_status(error.code)
    return not isinstance(error, ValueError)


def is_retriable_response(result: Any) -> bool:
    return isinstance(result, HttpResponse) and is_retriable_status(result.status)


def stop_on_shutdown(state: RetryCallState) -> bool:
    if is_shutdown_requested():
        logger.info("Shutdown requested; not retrying the failed request")
        return True
    return False


def last_outcome(state: RetryCallState) -> Any:
    return state.outcome.result()


def call_with_retry(call: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    retrying: Retrying = type.__call__(
        Retrying,
        retry=type.__call__(
            retry_any,
            type.__call__(retry_if_exception, predicate=is_retriable),
            type.__call__(retry_if_result, predicate=is_retriable_response),
        ),
        stop=type.__call__(
            stop_any,
            stop_on_shutdown,
            type.__call__(stop_after_delay, max_delay=TOTAL_SECONDS),
        ),
        wait=type.__call__(wait_fixed, wait=WAIT_SECONDS),
        retry_error_callback=last_outcome,
    )
    return retrying(call, *args, **kwargs)
