#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from tabsdatak.conn.common._stream._lifecycle import StreamLifecycle
from tabsdatak.conn.common._stream._offset import StreamOffset
from tabsdatak.conn.common._stream._offsets import OffsetKeeper
from tabsdatak.spi import StreamStateSpec


class StreamIngester[R, O: StreamOffset](ABC):
    offsets: OffsetKeeper[O]

    def __init__(self) -> None:
        # Resume mark seeded by :meth:`seed_offset` before the stream
        # opens; subclasses retype it to their concrete offset class.
        self._mark: O | None = None
        self.lifecycle: StreamLifecycle[O] = StreamLifecycle()

    def open_session(self, work_dir: Path) -> Any: ...

    @abstractmethod
    def close_session(self, session: Any) -> None: ...

    @abstractmethod
    def base_offset(self) -> O: ...

    @abstractmethod
    def poll_changes(self, session: Any) -> Iterable[R]: ...

    def staged_offset(self, offset: O) -> O:
        return offset

    def stage_state(self, offset: O) -> StreamStateSpec:
        return self.offsets.stage_state(self.staged_offset(offset))

    def resume_offset(
        self,
        *,
        first_stage: bool,
        stream_state: dict | None,
    ) -> O | None:
        return self.offsets.resume_offset(
            first_stage=first_stage,
            stream_state=stream_state,
        )

    def acknowledge_offset(self, offset: O) -> None:
        self.offsets.acknowledge_offset(offset)

    def attach_lifecycle(self, lifecycle: StreamLifecycle[O]) -> None:
        """Hand the ingester the run's lifecycle: what it raises events
        toward. An ingester whose source moves the stream's offset on by
        itself, past data with nothing to stage, reports each move through
        ``lifecycle.checkpoint``, so the recorded offset keeps up with the
        source's; one whose source takes work away from it (a Kafka
        partition revoked) asks for everything pending to be staged and
        acknowledged through ``lifecycle.settle``. Called before
        :meth:`open_session`."""
        self.lifecycle = lifecycle

    def settle_offset(self) -> None:
        """Called on a clean stop, after the last stage and before
        :meth:`close_session`: report any move of the stream's offset
        not recorded yet through ``lifecycle.checkpoint``."""

    def seed_offset(self, offset: O | None) -> None:
        """Seed the resume mark before the stream opens, so
        ``open_session`` / ``init_stream`` continues from ``offset``
        instead of ``start_from``. ``None`` leaves the mark untouched
        (cold start / source-managed resume)."""
        if offset is not None:
            self._mark = offset
