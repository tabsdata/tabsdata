#
#  Copyright 2026 Tabs Data Inc.
#

"""Thread-safe metadata writer.

A single daemon thread consumes ``MetaMessage`` objects from a
``queue.Queue`` and persists the aggregated ``MetaState`` to
``_meta.json`` using atomic writes.  Messages are debounced so that
the file is written at most once per *debounce_ms* milliseconds.

All tracker modules call ``enqueue()`` (non-blocking) from any hook or
thread.  The writer serializes all updates so there is never concurrent
access to the metadata file.
"""

from __future__ import annotations

import json
import os
import queue
import threading
import time
import warnings
from dataclasses import dataclass

from tabsdatax.pytest.plugins.harvester.internal.constants import (
    META_DEBOUNCE_MS,
)
from tabsdatax.pytest.plugins.harvester.internal.helpers import atomic_write
from tabsdatax.pytest.plugins.harvester.internal.schema import MetaState


@dataclass
class MetaMessage:
    """Tagged message for the writer queue.

    *kind* selects which part of MetaState to update.
    *payload* carries the data to merge.
    """

    kind: str  # "phase" | "fixture" | "marker" | "plugin" | "full_sync"
    key: str | None = None  # dict key inside the relevant section
    payload: dict | None = None  # fields to update


_SENTINEL = MetaMessage(kind="_stop")


class MetaWriter:
    """Queue-based single-writer for ``_meta.json``."""

    def __init__(
        self,
        meta_path: str,
        debounce_ms: int = META_DEBOUNCE_MS,
        fresh: bool = False,
    ) -> None:
        self._meta_path = meta_path
        self._debounce_s = debounce_ms / 1000.0
        self._queue: queue.Queue[MetaMessage] = queue.Queue()
        self._state = MetaState()
        self._thread: threading.Thread | None = None
        self._flush_event = threading.Event()
        self._stopped = threading.Event()
        if not fresh:
            self._load_existing()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def state(self) -> MetaState:
        return self._state

    def start(self) -> None:
        """Start the writer daemon thread."""
        if self._thread is not None:
            return
        self._stopped.clear()
        self._thread = threading.Thread(
            target=self._writer_loop,
            name="meta-writer",
            daemon=True,
        )
        self._thread.start()

    def enqueue(self, msg: MetaMessage) -> None:
        """Non-blocking: put a message for the writer thread."""
        self._queue.put_nowait(msg)

    def flush(self) -> None:
        """Block until the queue is drained and a final write is done."""
        self._flush_event.set()
        self._queue.put_nowait(MetaMessage(kind="_flush"))
        # Wait for the writer to signal completion
        deadline = time.monotonic() + 5.0
        while not self._flush_event.wait(timeout=0.1):
            if time.monotonic() > deadline:
                break

    def stop(self) -> None:
        """Gracefully stop the writer thread."""
        self._queue.put_nowait(_SENTINEL)
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load_existing(self) -> None:
        """Load previously persisted state for resume support."""
        if not os.path.exists(self._meta_path):
            return
        try:
            with open(self._meta_path) as f:
                data = json.load(f)
            if data.get("version", 1) >= 2:
                self._state = MetaState.from_dict(data)
        except (OSError, json.JSONDecodeError, KeyError):
            pass

    def _writer_loop(self) -> None:
        """Main loop: drain queue, apply messages, debounce writes."""
        last_write = 0.0
        dirty = False

        while True:
            try:
                msg = self._queue.get(timeout=self._debounce_s)
            except queue.Empty:
                if dirty:
                    self._persist()
                    dirty = False
                    last_write = time.monotonic()
                continue

            if msg.kind == "_stop":
                # Drain remaining messages before exiting
                while not self._queue.empty():
                    try:
                        remaining = self._queue.get_nowait()
                        if remaining.kind not in ("_stop", "_flush"):
                            self._apply(remaining)
                            dirty = True
                    except queue.Empty:
                        break
                if dirty:
                    self._persist()
                self._stopped.set()
                return

            if msg.kind == "_flush":
                # Drain and persist
                while not self._queue.empty():
                    try:
                        remaining = self._queue.get_nowait()
                        if remaining.kind not in ("_stop", "_flush"):
                            self._apply(remaining)
                    except queue.Empty:
                        break
                self._persist()
                dirty = False
                last_write = time.monotonic()
                self._flush_event.set()
                continue

            self._apply(msg)
            dirty = True

            now = time.monotonic()
            if now - last_write >= self._debounce_s:
                self._persist()
                dirty = False
                last_write = now

    def _apply(self, msg: MetaMessage) -> None:
        """Apply a message to the in-memory MetaState."""
        if msg.kind == "full_sync":
            # Full state replacement (used during collection)
            if msg.payload:
                for attr, value in msg.payload.items():
                    if hasattr(self._state, attr):
                        setattr(self._state, attr, value)
            return

        section = getattr(self._state, msg.kind + "s", None)
        if section is None or not isinstance(section, dict):
            return

        if msg.key is None:
            return

        if msg.key not in section:
            # Auto-create from payload using the appropriate dataclass
            from tabsdatax.pytest.plugins.harvester.internal.schema import (
                ComponentState,
                FixtureState,
                MarkerState,
                PhaseState,
                PluginState,
            )

            factories = {
                "phase": PhaseState,
                "fixture": FixtureState,
                "marker": MarkerState,
                "component": ComponentState,
                "plugin": PluginState,
            }
            factory = factories.get(msg.kind)
            if factory and msg.payload:
                section[msg.key] = factory.from_dict(msg.payload)
            elif factory:
                section[msg.key] = factory()
            return

        entry = section[msg.key]
        if msg.payload:
            for field_name, value in msg.payload.items():
                if hasattr(entry, field_name):
                    setattr(entry, field_name, value)

    def _persist(self) -> None:
        """Atomic write of the current state to disk."""
        try:
            data = self._state.to_dict()
            tmp = self._meta_path + ".tmp"
            atomic_write(tmp, self._meta_path, data)
        except OSError as e:
            warnings.warn(
                f"Harvester: failed to persist _meta.json: {e}",
                stacklevel=2,
            )
