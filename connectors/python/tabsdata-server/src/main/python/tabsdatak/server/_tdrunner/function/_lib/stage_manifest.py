#
# Copyright 2026 Tabs Data Inc.
#

"""Stage-manifest schema + IO.

Each stage manifest is one file per batch produced by the streamer
pod's ``StreamerPluginCtx.stage(...)`` call. The format matches the
contract that ``ManifestStageSrcPlugin`` (in
``tabsdata-conn-stream-stage``) reads on the publisher side, so the
streamer's writer and the publisher's reader agree by construction.

Layout (see :mod:`stage_layout`):

    <stage_root>/
        manifests/
            stream-manifest-<TS_ISO8601>.json
        files/
            <TS_ISO8601>/
                ...

Filename: ``stream-manifest-<ISO-8601-us>.json`` -- for example,
``stream-manifest-2026-06-16T10-30-12.345678.json``. The
ISO-8601-with-hyphenated-time portion is filesystem-safe (no ``:`` on
Windows) and sorts lexicographically by time, so a simple ``sorted()``
yields chronological order. Always UTC, no timezone suffix.

Precision is microseconds (6-digit fractional seconds) rather than
the more common milliseconds: two back-to-back ``ctx.stage()`` calls
on the same host can collide at ms precision (`datetime.now()` clock
ticks at ~1ms granularity on modern systems), which would overwrite
the first manifest. Microseconds make collisions effectively
impossible under any realistic streamer load.

Content: JSON, ``list[list[str]]``. The outer list is positional by
output table (slot ``i`` -> table ``i``); the inner list is the parquet
file paths the streamer wrote for that slot in this batch. Paths are
relative to the stage root, so the publisher's ``ManifestStageSrcPlugin``
resolves them against its conn's ``base_path`` (set to ``<stage_root>``);
a typical entry looks like ``files/<TS>/people-<TS>.parquet``, where the
``<TS>`` subdir matches this manifest's filename timestamp (see
``stage_layout.manifest_files_dir``).

The manifest-trigger sidecar calls :func:`external_uid_from_filename`
to extract the timestamp portion of the filename and passes it as
``?external_id=<TS>`` for apiserver-side dedup. The manifest content
itself carries no such field -- the filename is the authority.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from tabsdatak.server._tdrunner.function._lib.stage_layout import (
    manifests_dir,
)

MANIFEST_PREFIX = "stream-manifest-"
MANIFEST_SUFFIX = ".json"

# Strict ISO-8601-us with ``-`` instead of ``:`` and no timezone.
# Example: 2026-06-16T10-30-12.345678
_FILENAME_TS_PATTERN = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})"
    r"T(?P<hr>\d{2})-(?P<mn>\d{2})-(?P<sc>\d{2}\.\d{6})$"
)


class InvalidStageManifestError(ValueError):
    """The stage manifest filename or content is malformed."""


# --- Filename ops --------------------------------------------------------


def format_manifest_filename(timestamp: datetime) -> str:
    """Render ``timestamp`` as the canonical stream-manifest filename.

    Strips tzinfo (canonical format is naive-UTC) and emits
    microsecond-precision ISO-8601 with ``-`` in place of ``:`` so the
    name is filesystem-safe. The resulting string sorts
    lexicographically by chronological order.
    """
    if timestamp.tzinfo is not None:
        timestamp = timestamp.astimezone(timezone.utc).replace(tzinfo=None)
    iso = timestamp.isoformat(timespec="microseconds").replace(":", "-")
    return f"{MANIFEST_PREFIX}{iso}{MANIFEST_SUFFIX}"


def parse_manifest_filename(filename: str) -> datetime:
    """Inverse of :func:`format_manifest_filename`.

    Returns a naive datetime (canonical filename carries no tz; the
    convention is UTC). Callers that need a tz-aware datetime attach
    :attr:`datetime.timezone.utc` themselves.

    Raises:
        InvalidStageManifestError: if ``filename`` does not match the
            documented format.
    """
    if not filename.startswith(MANIFEST_PREFIX):
        raise InvalidStageManifestError(
            f"stage manifest filename {filename!r} missing {MANIFEST_PREFIX!r} prefix"
        )
    if not filename.endswith(MANIFEST_SUFFIX):
        raise InvalidStageManifestError(
            f"stage manifest filename {filename!r} missing {MANIFEST_SUFFIX} suffix"
        )
    stem = filename[len(MANIFEST_PREFIX) : -len(MANIFEST_SUFFIX)]
    m = _FILENAME_TS_PATTERN.match(stem)
    if m is None:
        raise InvalidStageManifestError(
            f"stage manifest filename {filename!r} is not in the expected "
            "ISO-8601-us format (e.g. "
            "stream-manifest-2026-06-16T10-30-12.345678.json)"
        )
    iso = f"{m.group('date')}T{m.group('hr')}:{m.group('mn')}:{m.group('sc')}"
    try:
        return datetime.fromisoformat(iso)
    except ValueError as exc:
        raise InvalidStageManifestError(
            f"stage manifest filename {filename!r} is not a valid timestamp: {exc}"
        ) from exc


def external_uid_from_filename(filename: str) -> str:
    """Return the ISO-8601 timestamp portion of ``filename``.

    The manifest-trigger sidecar passes this verbatim to the apiserver
    as ``?external_id=<TS>`` for dedup. Raises
    :class:`InvalidStageManifestError` if the filename does not match
    the canonical format.
    """
    # Validate via parse → discard the datetime; we want the raw string
    # back so apiserver-side equality with the manifest filename stem
    # is exact.
    parse_manifest_filename(filename)
    return filename[len(MANIFEST_PREFIX) : -len(MANIFEST_SUFFIX)]


# --- IO ------------------------------------------------------------------


def load_stage_manifest(path: Path) -> list[list[str]]:
    """Read and validate the stage manifest at ``path``.

    Returns a positional per-slot list of parquet file paths -- the
    same shape ``ManifestStageSrcPlugin`` consumes on the publisher
    side. Paths are returned as strings (verbatim from JSON); the
    convention is that they are relative to the stage root.

    Raises:
        FileNotFoundError: ``path`` does not exist.
        InvalidStageManifestError: JSON is malformed or does not have
            the ``list[list[str]]`` shape.
    """
    if not path.is_file():
        raise FileNotFoundError(f"stage manifest not found: {path}")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise InvalidStageManifestError(
            f"malformed JSON in stage manifest {path}: {exc}"
        ) from exc
    if not isinstance(raw, list) or not all(
        isinstance(slot, list) and all(isinstance(p, str) for p in slot) for slot in raw
    ):
        raise InvalidStageManifestError(
            f"stage manifest {path} must be a JSON list of lists of strings; "
            f"got {type(raw).__name__}"
        )
    return raw


def write_stage_manifest(
    stage_root: Path,
    tables_files: list[list[str]],
    *,
    ts: datetime,
) -> Path:
    """Serialise ``tables_files`` under ``<stage_root>/manifests/`` with
    a timestamp-derived filename.

    Creates ``<stage_root>/manifests/`` if it doesn't exist. Returns
    the absolute manifest path the data was written to.

    Writing through a tmp + replace is the streamer's responsibility
    (it owns the durability boundary) -- this function performs a
    direct ``write_text`` because at the layer where this is called
    the contents are small (paths, no payload) and a single ``write``
    is atomic on POSIX. NU4 wraps this in atomic-write semantics
    if needed.

    Raises:
        OSError: the file cannot be written or the parent directory
            cannot be created.
    """
    target_dir = manifests_dir(stage_root)
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / format_manifest_filename(ts)
    path.write_text(json.dumps(tables_files), encoding="utf-8")
    return path


def enumerate_pending(stage_root: Path) -> list[Path]:
    """Return all stream-manifest paths under ``<stage_root>/manifests/``
    in chronological order.

    Filters to files whose name parses via :func:`parse_manifest_filename`
    (so a parquet or a debris file dropped under ``manifests/`` won't
    derail the sidecar). Sorts lexicographically -- equivalent to
    chronologically, by construction of the filename format.

    Returns an empty list if ``<stage_root>/manifests/`` does not exist
    (the streamer hasn't run yet) or ``stage_root`` itself is missing.
    """
    target_dir = manifests_dir(stage_root)
    if not target_dir.is_dir():
        return []
    out: list[Path] = []
    for child in target_dir.iterdir():
        if not child.is_file():
            continue
        if not child.name.startswith(MANIFEST_PREFIX):
            continue
        if not child.name.endswith(MANIFEST_SUFFIX):
            continue
        try:
            parse_manifest_filename(child.name)
        except InvalidStageManifestError:
            continue
        out.append(child)
    out.sort(key=lambda p: p.name)
    return out


__all__ = [
    "MANIFEST_PREFIX",
    "MANIFEST_SUFFIX",
    "InvalidStageManifestError",
    "enumerate_pending",
    "external_uid_from_filename",
    "format_manifest_filename",
    "load_stage_manifest",
    "parse_manifest_filename",
    "write_stage_manifest",
]
