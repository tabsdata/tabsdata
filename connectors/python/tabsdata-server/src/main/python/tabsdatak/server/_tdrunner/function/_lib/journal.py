#
# Copyright 2026 Tabs Data Inc.
#

"""Journal — shared run-metadata file the chain reads and writes.

Every command in a function-run pod chain (publisher / subscriber /
transformer) receives ``--journal-file PATH`` pointing at a
single YAML file on the shared volume. ``begin-execution`` writes the
file at the start of the run; later commands may load, mutate, and
re-save it as the run progresses. The schema is grow-by-key — fields
are added incrementally as new startup-phase information becomes
available; readers ignore unknown keys.

``start`` and ``end`` are Rust i64 unix timestamps (milliseconds since
epoch) matching the ``start`` / ``end`` fields of
``ResponseMessagePayload`` in ``td-common/src/server.rs``, so the values
can flow through the chain into the eventual ``report-completion`` POST
body without an extra conversion. ``begin-execution`` writes ``start``;
a later step stamps ``end`` when the run finishes.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, fields
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


@dataclass
class Journal:
    """Shared metadata about the current run.

    Mutable on purpose: later steps will append fields as the chain
    grows. ``load`` / ``save`` are the canonical IO surface — callers
    should not hand-roll YAML against this file.
    """

    # Unix epoch milliseconds at which ``begin-execution`` recorded the
    # start of the run. Matches the i64 ``start`` field that the
    # apiserver callback envelope expects. ``None`` until written, so a
    # fresh ``Journal`` (the lenient ``load`` fallback) reports an
    # unknown start.
    start: int | None = None

    # Unix epoch milliseconds at which the run finished. Written by a
    # later step in the chain; ``None`` until then. Matches the i64
    # ``end`` field on the apiserver callback envelope.
    end: int | None = None

    @classmethod
    def load(cls, path: Path, *, strict: bool) -> "Journal":
        """Read ``path`` and return a ``Journal``.

        Unknown keys in the file are ignored so older readers stay
        compatible with files written by newer writers (forward-compat
        is by-design — the schema grows additively).

        With ``strict=True`` any failure — a missing or unreadable file,
        or malformed YAML — propagates. With ``strict=False`` the same
        failures are swallowed, logged, and a fresh default ``Journal``
        is returned, for callers that must not let a missing or broken
        journal get in their way (e.g. the failure-reporting path).
        """
        if not strict:
            # noinspection PyBroadException
            try:
                return cls.load(path, strict=True)
            except Exception:
                logger.warning(
                    f"Could not load journal at {path}; using defaults",
                    exc_info=True,
                )
                return cls()
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        known = {f.name for f in fields(cls)}
        kwargs = {k: v for k, v in raw.items() if k in known}
        return cls(**kwargs)

    def save(self, path: Path) -> None:
        """Serialise to YAML at ``path``.

        Creates parent directories if missing — the shared volume's
        ``journal/`` subdir is provided by the image template,
        but the file itself does not exist until ``begin-execution``
        writes it.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(asdict(self), sort_keys=False))
