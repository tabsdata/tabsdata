#
# Copyright 2026 Tabs Data Inc.
#

"""ExecutionMeta — shared run-metadata file the chain reads and writes.

Every command in a function-run pod chain (publisher / subscriber /
transformer) receives ``--execution-meta-file PATH`` pointing at a
single YAML file on the shared volume. ``begin-execution`` writes the
file at the start of the run; later commands may load, mutate, and
re-save it as the run progresses. The schema is grow-by-key — fields
are added incrementally as new startup-phase information becomes
available; readers ignore unknown keys.

``start`` is the Rust i64 unix timestamp (seconds since epoch). It
matches ``ResponseMessagePayload.start`` from
``td-common/src/server.rs``, so the value can flow through the chain
into the eventual ``report-completion`` POST body without an extra
conversion.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from pathlib import Path

import yaml


@dataclass
class ExecutionMeta:
    """Shared metadata about the current run.

    Mutable on purpose: later steps will append fields as the chain
    grows. ``load`` / ``save`` are the canonical IO surface — callers
    should not hand-roll YAML against this file.
    """

    # Unix epoch seconds at which ``begin-execution`` recorded the
    # start of the run. Matches the i64 ``start`` field that the
    # apiserver callback envelope expects.
    start: int

    @classmethod
    def load(cls, path: Path) -> "ExecutionMeta":
        """Read ``path`` and return an ``ExecutionMeta``.

        Unknown keys in the file are ignored so older readers stay
        compatible with files written by newer writers (forward-compat
        is by-design — the schema grows additively).
        """
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        known = {f.name for f in fields(cls)}
        kwargs = {k: v for k, v in raw.items() if k in known}
        return cls(**kwargs)

    def save(self, path: Path) -> None:
        """Serialise to YAML at ``path``.

        Creates parent directories if missing — the shared volume's
        ``execution_meta/`` subdir is provided by the image template,
        but the file itself does not exist until ``begin-execution``
        writes it.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(asdict(self), sort_keys=False))
