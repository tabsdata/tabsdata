#
#  Copyright 2026 Tabs Data Inc.
#

"""PhaseTracker — sequential pytest session phase state machine.

Tracks the six ordered phases of a pytest session and records
start/end timestamps for each transition.
"""

from __future__ import annotations

import time

from tabsdatax.pytest.plugins.harvester.internal.constants import (
    SESSION_PHASES_ORDERED,
)
from tabsdatax.pytest.plugins.harvester.internal.schema import PhaseState
from tabsdatax.pytest.plugins.harvester.internal.writer import MetaMessage, MetaWriter


class PhaseTracker:
    """Records transitions between sequential pytest session phases."""

    def __init__(self, writer: MetaWriter) -> None:
        self._writer = writer
        self._current: str | None = None
        self._start_times: dict[str, float] = {}
        # Initialize all phases as pending
        for phase in SESSION_PHASES_ORDERED:
            writer.enqueue(
                MetaMessage(
                    kind="phase",
                    key=phase,
                    payload=PhaseState().to_dict(),
                )
            )

    def enter_phase(self, phase: str) -> None:
        """Mark *phase* as active, closing the previous phase if open."""
        now = time.time()
        if self._current and self._current != phase:
            self._close_phase(self._current, now)
        self._current = phase
        self._start_times[phase] = now
        self._writer.enqueue(
            MetaMessage(
                kind="phase",
                key=phase,
                payload={"status": "active", "start": now},
            )
        )

    def exit_phase(self, phase: str) -> None:
        """Mark *phase* as done."""
        self._close_phase(phase, time.time())
        if self._current == phase:
            self._current = None

    def transition(self, new_phase: str) -> None:
        """Close the current phase and enter *new_phase*."""
        now = time.time()
        if self._current:
            self._close_phase(self._current, now)
        self._current = new_phase
        self._start_times[new_phase] = now
        self._writer.enqueue(
            MetaMessage(
                kind="phase",
                key=new_phase,
                payload={"status": "active", "start": now},
            )
        )

    def ensure(self, phase: str) -> None:
        """Enter *phase* only if it is not already active."""
        if self._current != phase:
            self.transition(phase)

    def finish_all(self) -> None:
        """Close any remaining open phase (called at unconfigure)."""
        if self._current:
            self._close_phase(self._current, time.time())
            self._current = None

    def _close_phase(self, phase: str, end_ts: float) -> None:
        start = self._start_times.get(phase)
        duration = (end_ts - start) if start is not None else None
        self._writer.enqueue(
            MetaMessage(
                kind="phase",
                key=phase,
                payload={
                    "status": "done",
                    "end": end_ts,
                    "duration": duration,
                },
            )
        )
