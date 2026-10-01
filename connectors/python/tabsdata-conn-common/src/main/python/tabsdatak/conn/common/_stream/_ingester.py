#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from tabsdatak.conn.common._stream._offset import StreamOffset
from tabsdatak.spi import StreamStateSpec


class StreamIngester[R, O: StreamOffset](ABC):
    def __init__(self) -> None:
        # Resume mark seeded by :meth:`seed_offset` before the stream
        # opens; subclasses retype it to their concrete offset class.
        self._mark: O | None = None
        self._checkpoint: Callable[[O], None] | None = None

    def open_session(self, work_dir: Path) -> Any: ...

    @abstractmethod
    def close_session(self, session: Any) -> None: ...

    @abstractmethod
    def base_offset(self) -> O: ...

    @abstractmethod
    def commit_offset(self, offset: O) -> None: ...

    @abstractmethod
    def poll_changes(self, session: Any) -> Iterable[R]: ...

    @abstractmethod
    def commit_state(self, offset: O) -> StreamStateSpec: ...

    @abstractmethod
    def resume_offset(
        self,
        *,
        first_stage: bool,
        stream_state: dict | None,
    ) -> O | None: ...

    def watch_position(self, checkpoint: Callable[[O], None]) -> None:
        """Hand the ingester what records a resume offset between stages.
        An ingester whose source moves the stream's position on by itself,
        past data with nothing to stage, reports each move through it, so
        the recorded offset keeps up with the source's. Called before
        :meth:`open_session`."""
        self._checkpoint = checkpoint

    def settle(self) -> None:
        """Called on a clean stop, after the last stage and before
        :meth:`close_session`: report any move of the stream's position
        not recorded yet through the :meth:`watch_position` checkpoint."""

    def seed_offset(self, offset: O | None) -> None:
        """Seed the resume mark before the stream opens, so
        ``open_session`` / ``init_stream`` continues from ``offset``
        instead of ``start_from``. ``None`` leaves the mark untouched
        (cold start / source-managed resume)."""
        if offset is not None:
            self._mark = offset
