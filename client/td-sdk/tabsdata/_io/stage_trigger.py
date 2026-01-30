#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import os
import shutil
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Literal, TypeAlias, Union

import requests

from tabsdata.exceptions import ErrorCode, StageTriggerConfigurationError

TRIGGER_FILE_PREFIX = "TD_INTERNAL_TRIGGER_FILE_"


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
        Defines the parameters for a stage trigger of a publisher.
        The source plugin being used must support stage trigger functionality.

        Args:
            start: datetime for the stage trigger to start working.
            end: datetime for the stage trigger to stop working.
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
            if start is not None and not start.tzinfo:
                raise StageTriggerConfigurationError(ErrorCode.STE4, start)
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
            if end is not None and not end.tzinfo:
                raise StageTriggerConfigurationError(ErrorCode.STE5, end)
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
        Continuously runs the stage trigger logic using the received context to
        trigger and monitor executions.
        SUCCESS if the StageTrigger.end datetime is reached. exit with 0.
        ERROR if any issue stops it from working or recovering. exit with 1.
        STOPPED upon a signal. exit with 0.
        """
        pass


class CallConfig:

    def __init__(
        self,
        method: Literal["GET", "POST", "PUT"],
        url: str,
        headers: dict,
        body: Any,
    ):
        self.method = method
        self.url = url
        self.headers = headers
        self.allows_body = body

    def call(self, body=None):
        url = self.url
        headers = self.headers
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
            f"CallConfig(method={self.method!r}, url={self.url!r},"
            f" headers={self.headers!r}, allows_body={self.allows_body!r})"
        )


FilesToStageSpec: TypeAlias = Union[Path | list[Path]]


class StageTriggerContext(ABC):
    """
    A context given to `StageTrigger` implementations to add files for a
    Stage publisher to consume and to trigger the Stage as needed.
    """

    @property
    @abstractmethod
    def working_dir(self) -> Path:
        """Returns the working dir to be used to store intermediate files."""

    @abstractmethod
    def trigger_monitor(self) -> bool:
        """
        Returns `True` if the publisher was triggered, `False` if not.

        Checks if there's a trigger pending because of a `trigger()`
        invocation while the Stage publisher was running. If so, it tries to
        trigger it again, resetting the trigger pending flag only if the
        trigger was successful.

        It must be invoked periodically by the StageTrigger.run().
        """

    @abstractmethod
    def is_active(self) -> bool:
        """
        Verifies if the run method of the Stage Trigger should continue executing.
        Usually, that will mean verifying if the end datetime of the Stage Trigger has
        been reached. Will return True if the method should continue running,
        or False otherwise (for example if the end datetime has been reached).
        """

    @abstractmethod
    def stage(self, parquet_files: FilesToStageSpec):
        """Writes the given files to the Stage consumption folder"""

    @abstractmethod
    def trigger(self) -> bool:
        """
        Returns `True` if the publisher was triggered, `False` if not.

        Attempts to trigger the associated Stage publisher function.
        The Stage publisher is triggered only if it is not currently
        running because of a previous stage trigger invocation.

        If the Stage publisher is currently running, it will only set a
        trigger pending flag. The `trigger_monitor()` will trigger the
        publisher once the previous execution plan completes.

        Triggers pending are not cumulative, if this method is invoked
        multiple times while the publisher is running, the first
        `trigger_monitor()`  invocation that triggers the publisher will
        reset the trigger pending, regardless of how many times the trigger()
        method was invoked.
        """


class LocalFileStageTriggerContext(StageTriggerContext):

    def __init__(
        self,
        staging_folder: str,
        trigger_config: CallConfig,
        logger,
        working_dir: str,
        end: datetime,
    ):
        self.staging_folder = staging_folder
        self.trigger_config = trigger_config
        self.logger = logger
        self._working_dir = Path(working_dir)
        self.end = end

    @property
    def working_dir(self) -> Path:
        return self._working_dir

    def trigger_monitor(self) -> bool:
        self.logger.debug("Monitoring triggering of the function")
        self.logger.debug(
            "Looking for files indicating the need to trigger the function."
        )
        folder = Path(self.staging_folder)

        found_and_deleted = False
        # Use glob() to efficiently find all items matching the pattern
        for file_path in folder.glob(f"{TRIGGER_FILE_PREFIX}*"):
            try:
                # Ensure we are only deleting files, not directories
                if file_path.is_file():
                    self.logger.debug(f"Found matching file, deleting: {file_path}")
                    file_path.unlink()  # Deletes the file
                    found_and_deleted = True
            except OSError as e:
                # TODO: This is one of the pieces that should be rethought once there
                #  is a resiliency reimplementation
                self.logger.error(f"Error deleting file {file_path}: {e}")
                raise
        if found_and_deleted:
            self.logger.debug("A trigger was pending, trying to launch it.")
            return self.trigger()
        else:
            self.logger.debug("No trigger was pending. Nothing will be done.")
            return False

    def is_active(self) -> bool:
        time_now = datetime.now(tz=timezone.utc)
        if self.end is None or self.end > time_now:
            return True
        else:
            self.logger.info(
                f"The current time '{time_now}' is latter than the "
                f"configured end time '{self.end}'. The "
                "stage trigger should stop running."
            )
            return False

    def stage(self, parquet_files: FilesToStageSpec):
        # Ensure the destination directory exists. If not, create it.
        self.logger.debug(f"Staging files: {parquet_files}")
        os.makedirs(self.staging_folder, exist_ok=True)
        self.logger.debug(f"Destination folder: '{self.staging_folder}'")
        parquet_files = (
            parquet_files if isinstance(parquet_files, list) else [parquet_files]
        )
        for source_file in parquet_files:
            try:
                # Use shutil.copy2 to copy the file and its metadata
                shutil.copy2(source_file, self.staging_folder)
                self.logger.debug(f"  - Copied '{source_file}'")
            except FileNotFoundError:
                # TODO: This is one of the pieces that should be rethought once there
                #  is a resiliency reimplementation
                self.logger.error(
                    f"  - Error: Source file not found at '{source_file}'"
                )
                raise
            except Exception as e:
                # TODO: This is one of the pieces that should be rethought once there
                #  is a resiliency reimplementation
                self.logger.error(f"  - Error copying '{source_file}': {e}")
                raise
        self.logger.debug("All files staged successfully")

    def trigger(self) -> bool:
        self.logger.debug("Requesting trigger of the function")
        result = self.trigger_config.call(
            {
                "skip_if_running": False,
                "trigger_type": "ST",
            }
        )
        self.logger.debug(f"Result of the request: {result}")
        result.raise_for_status()
        success = _check_success(result)
        if not success:
            self.logger.debug(
                "The trigger request was not successful. This is not an "
                "error, but instead indicates that an execution of the "
                "function is already running. Marking the trigger of the "
                "function as pending."
            )
            trigger_file_name = (
                TRIGGER_FILE_PREFIX + str(uuid.uuid4().hex[:16]) + ".txt"
            )
            trigger_file_path = Path(
                os.path.join(self.staging_folder, trigger_file_name)
            )
            trigger_file_path.touch()
            self.logger.debug(
                f"Created file '{trigger_file_path}' to indicate that a "
                "trigger is pending for the function."
            )
        else:
            self.logger.debug("The function was triggered successfully.")
        return success

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(staging_f"
            f"older={self.staging_folder}, trigger_config={self.trigger_config}"
        )


def _check_success(result):
    result_json = result.json()
    data_dict = result_json.get("data")
    if not data_dict or len(data_dict.keys()) != 1:
        # TODO: This is one of the pieces that should be rethought once there
        #  is a resiliency reimplementation
        raise ValueError(
            "Trying to assess the success of a trigger attempt, "
            "but the resulting JSON does not have the required fields. "
            f"The JSON is: {result_json}"
        )
    result = next(iter(data_dict.keys()))
    return result == "Executed"
