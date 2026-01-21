#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, Union

import requests

from tabsdata.exceptions import ErrorCode, StageTriggerConfigurationError

if TYPE_CHECKING:
    pass


class StageTriggerStatus(Enum):
    """
    Status returned by the `run()` method of `StageTrigger` implementations.
    """

    SUCCESS = 0
    """
    The stage trigger finished successfully.
    """

    ERROR = 1
    """
    The stage trigger finished in error.
    """

    STOPPED = 2
    """
    The stage trigger finished because of a signal.
    """


class StageTrigger(ABC):
    def __init__(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
    ):
        """
        Defines the watchdog parameters for a watchdog of a publisher.
        The source plugin being used must support watchdog functionality.

        Args:
            start: datetime for the watchdog to start working.
            end: datetime for the watchdog to stop working.
        """

        self.start = start
        self.end = end

    @property
    def start(self) -> datetime | None:
        return self._start

    @start.setter
    def start(self, start: datetime | None):
        if start is None or isinstance(start, datetime):
            current_end = getattr(self, "_end", None)
            if start is not None and current_end is not None and start >= current_end:
                raise StageTriggerConfigurationError(ErrorCode.STE3, start, current_end)
            self._start = start
        else:
            raise StageTriggerConfigurationError(ErrorCode.STE1, start, type(start))

    @property
    def end(self) -> datetime | None:
        return self._end

    @end.setter
    def end(self, end: datetime | None):
        if end is None or isinstance(end, datetime):
            current_start = getattr(self, "_start", None)
            if current_start is not None and end is not None and current_start >= end:
                raise StageTriggerConfigurationError(ErrorCode.STE3, current_start, end)
            self._end = end
        else:
            raise StageTriggerConfigurationError(ErrorCode.STE2, end, type(end))

    def _to_dict(self) -> dict:
        return {
            "start": self.start.isoformat() if self.start else None,
            "end": self.end.isoformat() if self.end else None,
        }

    @abstractmethod
    def run(self, ctx: StageTriggerContext) -> StageTriggerStatus:
        """
        Continuously runs the watchdog logic using the received context to
        trigger and monitor executions.
        SUCCESS if the Watchdog.end datetime is reached. exit with 0.
        ERROR if any issue stops it from working or recovering. exit with 1.
        STOPPED upon a signal. exit with 0.
        """
        pass


# TODO THIS SHOULD DISAPPEAR FROM HERE INTO THE CONTEXT IMPL SUB-CLASS
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


FilesToStageSpec: TypeAlias = Union[Path | list[Path]]


class StageTriggerContext(ABC):
    """
    A context given to `StageTrigger` implementations to add files for a
    Stage publisher to consume and to trigger the Stage as needed.
    """

    def trigger_monitor(self) -> bool:
        """
        Returns `True` if the publisher was triggered, `False` if not.

        Checks if there s a trigger pending because of a `trigger()`
        invocation while the Stage publisher was running. If so, it tries to
        trigger it again, resetting the trigger pending flag only if the
        trigger was successful.

        It must be invoked periodically by the StageTrigger.run().
        """
        pass

    def stage(self, parquet_files: FilesToStageSpec):
        """Writes the given files to the Stage consumption folder"""
        pass

    def trigger(self) -> bool:
        """
        Returns `True` if the publisher was triggered, `False` if not.

        Attempts to trigger the associated Stage publisher function.
        The Stage publisher is triggered only if it is not currently
        running because of a previous stage trigger invocation.

        If the Stage publisher is currently running, it will only set a
        trigger pending flag. The `trigger_monitor()` will trigger the
        publisher once the previous execution plan completes.

        Trigger pendings are not cummulative, if this method is invoked
        multiple times while the publisher is running, the first
        `trigger_monitor()`  invocation that triggers the publisher will
        reset the trigger pending, regardless of how many times the trigger()
        method was invoked.
        """
        pass
