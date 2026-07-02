#
# Copyright 2026 Tabs Data Inc.
#

"""Filesystem layout conventions for the streamer's stage storage.

The streamer pod's ``StreamerPluginCtx`` and the publisher pod's
``ManifestStageSrcPlugin`` agree on a fixed directory structure under
the per-function ``function_stage`` root:

    <stage_root>/
        stream-state.json                              <- streamer-owned state
        manifests/
            stream-manifest-<TS_ISO8601>.json          <- one per ``ctx.stage()`` call
        files/
            <TS_ISO8601>/                              <- one subdir per manifest
                <table_a>-<TS>.parquet                 <- the staged data files
                <table_b>-<TS>.parquet
                ...

The parquets for a given batch live under ``files/<TS>/`` where ``<TS>``
is the timestamp portion of that batch's manifest filename -- so a
manifest and its data files share a name, and cleaning up an orphaned
batch is a single ``rmdir`` of its ``files/<TS>/`` directory rather than
a hunt through a flat ``files/`` shared by every batch. The
``manifests/`` / ``files/`` split itself is kept so the manifest stays
small (it carries paths, not payload); the per-manifest grouping applies
only *within* ``files/``.

This module pins those names and provides the envelope shape for the
``stream-state.json`` file. It is pure utility code: no I/O, no callers
yet -- consumers land in subsequent tickets (``_lib/stage_manifest.py``
for the manifest read/write, ``StreamerPluginCtx`` for the state file).
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

# --- Directory / file names -----------------------------------------------

MANIFESTS_SUBDIR = "manifests"
FILES_SUBDIR = "files"
STREAM_STATE_FILENAME = "stream-state.json"


def manifests_dir(stage_root: Path) -> Path:
    """``<stage_root>/manifests/`` -- where the streamer writes manifests
    and the publisher's ``ManifestStageSrcPlugin`` globs."""
    return stage_root / MANIFESTS_SUBDIR


def files_dir(stage_root: Path) -> Path:
    """``<stage_root>/files/`` -- the root the streamer writes parquets
    under; each batch gets its own :func:`manifest_files_dir` subdir."""
    return stage_root / FILES_SUBDIR


def manifest_files_dir(stage_root: Path, external_id: str) -> Path:
    """``<stage_root>/files/<external_id>/`` -- the per-batch subdir
    holding the parquets a single ``ctx.stage()`` call staged.

    ``external_id`` is the timestamp portion of that batch's manifest
    filename (see ``stage_manifest.external_uid_from_filename``), so the
    manifest ``stream-manifest-<TS>.json`` and its data files under
    ``files/<TS>/`` share a name. That pairing lets the manifest-trigger
    sidecar reclaim a whole batch with one directory removal and lets an
    operator eyeball which files belong to which manifest."""
    return files_dir(stage_root) / external_id


def stream_state_path(stage_root: Path) -> Path:
    """``<stage_root>/stream-state.json`` -- the single file holding the
    streamer's persisted state envelope."""
    return stage_root / STREAM_STATE_FILENAME


# --- stream-state.json envelope --------------------------------------------

STREAM_STATE_KIND = "streamState"
STREAM_STATE_API_VERSION = 1


class InvalidStateEnvelopeError(ValueError):
    """Raised when ``stream-state.json`` doesn't match the expected schema.

    Wraps :class:`pydantic.ValidationError` so callers can catch
    envelope-specific failures without catching unrelated ``ValueError``
    instances from surrounding JSON parsing.
    """


class StreamStateView(BaseModel):
    """Parsed view of a ``stream-state.json`` envelope.

    The wire format is camelCase; Python attributes are snake_case via
    pydantic field aliases. ``model_dump(by_alias=True)`` round-trips
    back to the on-wire dict shape.

    Strict on the structural fields (``kind``, ``apiVersion``, ``state``):
    a mismatch indicates either a schema migration we haven't taught the
    runtime about or a corrupted file -- both deserve a typed error so
    the streamer can fall back to first-run semantics rather than
    silently losing state. Tolerant on ``externalId`` and
    ``tabsdataVersion`` (treated as ``None`` when absent) since legacy
    files may predate those fields.

    Fields:

      * ``state`` -- the inner dict the user plugin reads / writes via
        :attr:`StreamSrcPluginCtx.state`.
      * ``external_id`` -- the manifest TS that produced this state (the
        streamer pairs each state write with the manifest emitted in the
        same ``ctx.stage()`` call). Diagnostic / cross-reference only;
        absent for legacy files.
      * ``tabsdata_version`` -- the Tabsdata runtime version that wrote
        the file. Lets future versions detect older envelopes and
        migrate the schema if it evolves. Absent for legacy files.
      * ``kind`` / ``api_version`` -- structural envelope fields.
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True, extra="forbid")

    state: dict
    external_id: str | None = Field(default=None, alias="externalId")
    tabsdata_version: str | None = Field(default=None, alias="tabsdataVersion")
    kind: Literal["streamState"]
    api_version: Literal[1] = Field(alias="apiVersion")


def format_state_envelope(
    state: dict,
    *,
    external_id: str,
    tabsdata_version: str,
) -> dict:
    """Build the ``stream-state.json`` envelope around ``state``.

    Caller (typically ``StreamerPluginCtx.stage()``) supplies the
    ``external_id`` (the TS of the manifest just written in the same
    ``stage()`` call) and the ``tabsdata_version`` of the running
    runtime. The returned dict is what gets ``json.dump``'d to disk.
    """
    return StreamStateView(
        state=state,
        external_id=external_id,
        tabsdata_version=tabsdata_version,
        kind=STREAM_STATE_KIND,
        api_version=STREAM_STATE_API_VERSION,
    ).model_dump(by_alias=True)


def parse_state_envelope(raw: dict) -> StreamStateView:
    """Parse the ``stream-state.json`` envelope into a
    :class:`StreamStateView`.

    Thin wrapper around :meth:`StreamStateView.model_validate` that
    converts pydantic's ``ValidationError`` into the module-local
    :class:`InvalidStateEnvelopeError`.

    Raises:
        InvalidStateEnvelopeError: if ``raw`` is not a dict, its
            ``kind`` is not ``streamState``, its ``apiVersion`` is not
            ``1``, or ``state`` is missing / not a dict.
    """
    try:
        return StreamStateView.model_validate(raw)
    except ValidationError as exc:
        raise InvalidStateEnvelopeError(str(exc)) from exc
