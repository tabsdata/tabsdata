#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from collections.abc import Callable

from tabsdatak.conn.common._stream._offset import StreamOffset
from tabsdatak.conn.common.error import ConnErrorCode
from tabsdatak.spi import StreamStateSpec

logger = logging.getLogger(__name__)

DEFAULT_ACKNOWLEDGE_ATTEMPTS = 3
DEFAULT_ACKNOWLEDGE_PAUSE_SECS = 1.0


class OffsetKeeper[O: StreamOffset](ABC):
    @abstractmethod
    def stage_state(self, offset: O) -> StreamStateSpec: ...

    @abstractmethod
    def resume_offset(
        self,
        *,
        first_stage: bool,
        stream_state: dict | None,
    ) -> O | None: ...

    def acknowledge_offset(self, offset: O) -> None:
        pass


class TabsdataKeptOffsets[O: StreamOffset](OffsetKeeper[O]):
    def __init__(
        self,
        *,
        key: str,
        read: Callable[[str], O],
        acknowledger: Callable[[O], None] | None = None,
    ):
        self.key = key
        self.read = read
        self.acknowledger = acknowledger

    def stage_state(self, offset: O) -> StreamStateSpec:
        return {self.key: offset.write()}

    def resume_offset(
        self,
        *,
        first_stage: bool,
        stream_state: dict | None,
    ) -> O | None:
        if first_stage or not stream_state:
            return None
        state = stream_state.get(self.key)
        if state is None:
            return None
        return self.read(state)

    def acknowledge_offset(self, offset: O) -> None:
        if self.acknowledger is None:
            return
        try:
            self.acknowledger(offset)
        except Exception as error:
            logger.warning(
                "The source did not take the acknowledgement of the staged offset; "
                "the offset is kept in the Tabsdata stream state, so only the "
                "retention at the source is affected: %s",
                error,
            )


class SourceKeptOffsets[O: StreamOffset](OffsetKeeper[O]):
    def __init__(
        self,
        *,
        acknowledger: Callable[[O], None],
        retriable: Callable[[BaseException], bool],
        attempts: int = DEFAULT_ACKNOWLEDGE_ATTEMPTS,
        pause: float = DEFAULT_ACKNOWLEDGE_PAUSE_SECS,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.acknowledger = acknowledger
        self.retriable = retriable
        self.attempts = attempts
        self.pause = pause
        self.sleep = sleep

    def stage_state(self, offset: O) -> StreamStateSpec:
        snapshot = type(offset).read(offset.write())
        return lambda: self.acknowledge_staged(snapshot)

    def resume_offset(
        self,
        *,
        first_stage: bool,
        stream_state: dict | None,
    ) -> O | None:
        return None

    def acknowledge_staged(self, offset: O) -> None:
        for attempt in range(1, self.attempts + 1):
            try:
                self.acknowledger(offset)
                return
            except Exception as error:
                if not self.retriable(error):
                    raise
                if attempt == self.attempts:
                    raise ConnErrorCode.CONN_35.exception(
                        cause=error,
                        attempts=attempt,
                        error=error,
                    )
                logger.warning(
                    "The acknowledgement of the staged offset failed on attempt "
                    "%s of %s and will be retried: %s",
                    attempt,
                    self.attempts,
                    error,
                )
                self.sleep(self.pause)
