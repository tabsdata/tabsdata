#
# Copyright 2026 Tabsdata Inc.
#

from typing import Callable, TypeVar

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_delay,
    wait_fixed,
)

from tabsdata.api.apiserver import APIServerError

from test_tabsdata.common.constants import EXECUTION_MAXIMUM_TIMEOUT_SECONDS

T = TypeVar("T")

__all__ = ["retry_until_pfr_bound"]


def retry_until_pfr_bound(
    callable_returning_response: Callable[[], T],
    *,
    timeout_seconds: int = EXECUTION_MAXIMUM_TIMEOUT_SECONDS,
    poll_seconds: float = 2,
) -> T:
    @retry(
        stop=stop_after_delay(timeout_seconds),
        wait=wait_fixed(poll_seconds),
        retry=retry_if_exception_type(APIServerError),
        reraise=True,
    )
    def runner():
        return callable_returning_response()

    return runner()
