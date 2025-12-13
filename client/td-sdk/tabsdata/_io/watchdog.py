#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal

import requests

from tabsdata.exceptions import ErrorCode, WatchDogConfigurationError

if TYPE_CHECKING:
    from tabsdata.api.status_utils.function_run import FunctionRunStatus


class WatchDog:
    def __init__(
        self,
        timeout_mins: int | None = None,
        size: int | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ):
        """
        Defines the watchdog parameters for a watchdog of a publisher.
        The source plugin being used must support watchdog functionality.

        Args:
            timeout_mins (int, optional): Time threshold in minutes for the watchdog to
                trigger the publisher.
            size (int, optional): Size threshold for a watchdog to trigger the
                publisher. Depending on the source being used the size may be a record
                count, data size, file count, etc. Refer to the source documentation
                for more information.
            start (datetime, optional): datetime for the watchdog to start working.
            end (datetime, optional): datetime for the watchdog to stop working.
        """

        self.timeout_mins = timeout_mins
        self.size = size
        self.start = start
        self.end = end

    @property
    def timeout_mins(self) -> int | None:
        return self._timeout_mins

    @timeout_mins.setter
    def timeout_mins(self, timeout_mins: int | None):
        if timeout_mins is None:
            self._timeout_mins = None
        elif isinstance(timeout_mins, int):
            if timeout_mins < 0:
                raise WatchDogConfigurationError(
                    ErrorCode.WCE1, timeout_mins, type(timeout_mins)
                )
            self._timeout_mins = timeout_mins
        else:
            raise WatchDogConfigurationError(
                ErrorCode.WCE1, timeout_mins, type(timeout_mins)
            )

    @property
    def size(self) -> int | None:
        return self._size

    @size.setter
    def size(self, size: int | None):
        if size is None:
            self._size = None
        elif isinstance(size, int):
            if size < 0:
                raise WatchDogConfigurationError(ErrorCode.WCE2, size, type(size))
            self._size = size
        else:
            raise WatchDogConfigurationError(ErrorCode.WCE2, size, type(size))

    @property
    def start(self) -> datetime | None:
        return self._start

    @start.setter
    def start(self, start: datetime | None):
        if start is None or isinstance(start, datetime):
            current_end = getattr(self, "_end", None)
            if start is not None and current_end is not None and start >= current_end:
                raise WatchDogConfigurationError(ErrorCode.WCE5, start, current_end)
            self._start = start
        else:
            raise WatchDogConfigurationError(ErrorCode.WCE3, start, type(start))

    @property
    def end(self) -> datetime | None:
        return self._end

    @end.setter
    def end(self, end: datetime | None):
        if end is None or isinstance(end, datetime):
            current_start = getattr(self, "_start", None)
            if current_start is not None and end is not None and current_start >= end:
                raise WatchDogConfigurationError(ErrorCode.WCE5, current_start, end)
            self._end = end
        else:
            raise WatchDogConfigurationError(ErrorCode.WCE4, end, type(end))

    def _to_dict(self) -> dict:
        return {
            "timeout_mins": self.timeout_mins,
            "size": self.size,
            "start": self.start.isoformat() if self.start else None,
            "end": self.end.isoformat() if self.end else None,
        }


class CallConfig:

    def __init__(
        self,
        method: Literal["GET", "POST", "PUT"],
        url: str,
        headers: list[list[str]],
        body: Any,
    ):
        self.method = method
        self.url = url
        self.headers = headers
        self.body = body

    def call(self):
        url = self.url
        body = self.body
        headers = {v[0]: v[1] for v in self.headers}
        if self.method == "POST":
            method = requests.post
        elif self.method == "PUT":
            method = requests.put
        elif self.method == "GET":
            method = requests.get
        else:
            raise ValueError(f"Invalid method: {self.method}")
        return method(url=url, headers=headers, json=body)

    def __repr__(self):
        return (
            f"CallConfig(method={self.method!r},"
            f" url={self.url!r},headers={self.headers!r}, body={self.body!r})"
        )


class SourceListenerContext:

    def __init__(
        self,
        state_config: CallConfig,
        status_config: CallConfig,
        trigger_config: CallConfig,
    ):
        self.state_config = state_config
        self.status_config = status_config
        self.trigger_config = trigger_config

    def trigger(self) -> str:
        """
        Returns the execution plan ID for the trigger call.
        """
        pass
        # return self.trigger_config.call().json().get("ID")

    def function_status(self, execution_plan_id: str | None) -> FunctionRunStatus:
        """
        Returns the status of the function in the given execution plan ID.
        """
        # new_url = self.status_config.url + f"/{execution_plan_id}"
        # self.status_config.url = new_url
        # return self.status_config.call().json()
        pass

    def function_state(self) -> dict | None:
        """
        Returns the current fn_state dictionary.
        NOTE: The impl will ask this to an apiserver internal (loopback) endpoint
        """
        # response = self.state_config.call().content
        # import polars as pl
        # df = pl.read_parquet(response) # If it can't be done in memory,
        # create temporary file
        # return df.to_dicts()
        pass

    def __repr__(self):
        return (
            f"SourceListenerContext(state_config={self.state_config!r},"
            f" status_config={self.status_config!r}, "
            f"trigger_config={self.trigger_config!r})"
        )


class ListenerStatus(Enum):
    SUCCESS = 0
    ERROR = 1
    STOPPED = 2


class SourceListener(ABC):

    def __init__(self, watchdog: WatchDog):
        """ "
        Initializes the watchdog with the given watchdog object.
        The Watchdog class is owned the SourcePlugin which can provide
        all the necessary connection and source contextual information
        """
        self.watchdog = watchdog

    @abstractmethod
    def listen(self, ctx: SourceListenerContext) -> ListenerStatus:
        """
        Continuously runs the watchdog logic using the received context to
        trigger and monitor executions.
        SUCCESS if the Watchdog.end datetime is reached. exit with 0.
        ERROR if any issue stops it from working or recovering. exit with 1.
        STOPPED upon a signal. exit with 0.
        """
        pass
