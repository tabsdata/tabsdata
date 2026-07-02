#
# Copyright 2026 Tabs Data Inc.
#

"""Stage-backend abstraction shared across the stream-stage components.

A :class:`StageBackend` wraps the single populated connector connection
(local / s3 / az / gcs) behind one scheme-agnostic surface, so callers
need not branch on the backend type:

  * :meth:`build_location` -- turn a stage-relative path into a
    transporter location;
  * :meth:`credential_env` -- the env the ``td_transporter`` binary
    needs to reach the backend (``None`` for local; the connector's
    ``run_env`` for cloud);
  * :attr:`is_object_store` -- lets callers gate filesystem-only
    behaviour. On an object store a "directory" is only a key prefix
    that disappears with its last object, so e.g. the manifest-trigger
    sidecar's per-batch ``rmdir`` is a local-filesystem-only step.

Today the only constructor is :func:`backend_for_conn`, used by the
publisher's :class:`~tabsdatak.conn.stream.stage.ManifestStageSrcPlugin`.
The streamer and sidecar adopt this surface (and grow ``Location``-based
construction plus ``upload`` / ``delete`` / ``next_oldest_manifest``
operations) as cloud staging lands -- see ``cloud-staging-design.md``.
"""

from __future__ import annotations

import shutil
from abc import ABC, abstractmethod
from pathlib import Path

from tabsdatak.common._transporter.handler.handler import TransporterHandler
from tabsdatak.conn.azureblob import AzureBlobSrcConn, _plugin as _azureblob_plugin
from tabsdatak.conn.gcs import GCSSrcConn, _plugin as _gcs_plugin
from tabsdatak.conn.localfile import LocalFileSrcConn, _plugin as _localfile_plugin
from tabsdatak.conn.s3 import S3SrcConn, _plugin as _s3_plugin
from tabsdatak.conn.stream.stage.error import ManifestStageErrorCode


class StageBackend(ABC):
    """Scheme-agnostic view of one stage backend (the populated slot of
    a ``ManifestStageConn``).

    ``build_location`` / ``credential_env`` / ``is_object_store`` are the
    read-side surface every backend supports. ``upload`` / ``read_text``
    are write-side capabilities used by the streamer; they default to
    ``NotImplementedError`` so a read-only backend need not implement
    them."""

    @property
    @abstractmethod
    def is_object_store(self) -> bool:
        """``True`` for cloud object stores (s3 / az / gcs), where a
        directory is only a key prefix; ``False`` for a local-filesystem
        stage with real directories."""

    @abstractmethod
    def build_location(self, rel: str, handler: TransporterHandler):
        """Resolve a stage-root-relative path into a transporter
        location built by ``handler``."""

    @abstractmethod
    def credential_env(self, handler: TransporterHandler) -> dict[str, str] | None:
        """The transporter binary's credential env for this backend, or
        ``None`` when none is required (local)."""

    def upload(self, local_path: Path, rel: str) -> None:
        """Publish a local file to the stage at stage-relative ``rel``
        (creating intermediate prefixes/dirs as needed)."""
        raise NotImplementedError(f"{type(self).__name__} does not support upload")

    def read_text(self, rel: str) -> str | None:
        """Read a small stage control file at ``rel`` as UTF-8 text, or
        ``None`` if it does not exist."""
        raise NotImplementedError(f"{type(self).__name__} does not support read_text")

    def next_after(
        self, glob: str, cursor: str | None, scratch_dir: Path
    ) -> tuple[Path, str] | None:
        """Fetch the oldest stage entry matching ``glob`` whose ordering
        key is strictly greater than ``cursor`` (``None`` = from the
        start). Returns ``(local_path, next_cursor)`` -- ``local_path`` is
        the entry on disk (downloaded into ``scratch_dir`` for a remote
        backend, preserving its basename), ``next_cursor`` the opaque
        token to pass back to advance past it -- or ``None`` if none
        remain. The cursor is backend-internal (filename for local,
        last-modified for cloud) and must not be interpreted by callers."""
        raise NotImplementedError(f"{type(self).__name__} does not support next_after")

    def delete(self, rels: list[str]) -> None:
        """Delete the given stage-relative paths (files, and -- on a
        local filesystem -- empty directories)."""
        raise NotImplementedError(f"{type(self).__name__} does not support delete")


class _LocalStageBackend(StageBackend):
    """Local-filesystem stage. The localfile connector's
    ``build_location`` applies ``base_path`` itself, so paths pass
    through verbatim, and no credentials are needed."""

    def __init__(self, conn: LocalFileSrcConn) -> None:
        self._conn = conn

    @property
    def is_object_store(self) -> bool:
        return False

    def build_location(self, rel: str, handler: TransporterHandler):
        return _localfile_plugin.build_location(self._conn, rel, handler)

    def credential_env(self, handler: TransporterHandler) -> dict[str, str] | None:
        return None

    def upload(self, local_path: Path, rel: str) -> None:
        # ``shutil.move`` is a rename on the same filesystem -- atomic and
        # cheap, matching the streamer's prior in-process behaviour.
        dst = Path(self._conn.base_path) / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(local_path), str(dst))

    def read_text(self, rel: str) -> str | None:
        src = Path(self._conn.base_path) / rel
        return src.read_text(encoding="utf-8") if src.is_file() else None

    def next_after(
        self, glob: str, cursor: str | None, scratch_dir: Path
    ) -> tuple[Path, str] | None:
        # Filenames sort chronologically by construction, so the name is a
        # natural, exact cursor (no last-modified coarseness). scratch_dir
        # is unused -- the local file is returned in place.
        base = Path(self._conn.base_path)
        for p in sorted(base.glob(glob), key=lambda p: p.name):
            if cursor is None or p.name > cursor:
                return (p, p.name)
        return None

    def delete(self, rels: list[str]) -> None:
        base = Path(self._conn.base_path)
        for rel in rels:
            p = base / rel
            if p.is_dir():
                p.rmdir()  # empty-only; raises if non-empty (caller logs)
            else:
                p.unlink(missing_ok=True)


class _CloudStageBackend(StageBackend):
    """Cloud object-store stage (s3 / az / gcs). The cloud connectors'
    ``build_location`` does not apply ``base_path``, so paths are
    resolved through ``conn.full_path`` first; ``run_env`` supplies the
    transporter's credential env."""

    def __init__(self, conn: S3SrcConn | AzureBlobSrcConn | GCSSrcConn, plugin) -> None:
        self._conn = conn
        self._plugin = plugin

    @property
    def is_object_store(self) -> bool:
        return True

    def build_location(self, rel: str, handler: TransporterHandler):
        return self._plugin.build_location(
            self._conn, self._conn.full_path(rel), handler
        )

    def credential_env(self, handler: TransporterHandler) -> dict[str, str] | None:
        return self._plugin.run_env(self._conn, handler)


def backend_for_conn(conn) -> StageBackend:
    """Resolve a stage conn into a :class:`StageBackend`.

    If ``conn`` carries a pre-built backend on a ``stage_backend``
    attribute (how the runner passes a backend it resolved from the
    ``function_stage`` ``Location`` -- e.g. a cloud stage built via
    ``_lib.stage_backend``), it is returned as-is. Otherwise the single
    populated connector slot (``local`` / ``s3`` / ``az`` / ``gcs``) is
    wrapped; exactly one slot must be populated, else ``MANIFEST_STAGE_1``.
    """
    prebuilt = getattr(conn, "stage_backend", None)
    if prebuilt is not None:
        return prebuilt
    slots = [
        (conn.local, _localfile_plugin, False),
        (conn.s3, _s3_plugin, True),
        (conn.az, _azureblob_plugin, True),
        (conn.gcs, _gcs_plugin, True),
    ]
    populated = [
        (c, plugin, is_cloud) for c, plugin, is_cloud in slots if c is not None
    ]
    if len(populated) != 1:
        raise ManifestStageErrorCode.MANIFEST_STAGE_1.exception(count=len(populated))
    backend_conn, plugin, is_cloud = populated[0]
    if is_cloud:
        return _CloudStageBackend(backend_conn, plugin)
    return _LocalStageBackend(backend_conn)
