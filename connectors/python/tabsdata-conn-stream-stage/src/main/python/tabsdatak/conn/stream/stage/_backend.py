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

from abc import ABC, abstractmethod

from tabsdatak.conn.azureblob import AzureBlobSrcConn, _plugin as _azureblob_plugin
from tabsdatak.conn.common.file import _transporter
from tabsdatak.conn.gcs import GCSSrcConn, _plugin as _gcs_plugin
from tabsdatak.conn.localfile import LocalFileSrcConn, _plugin as _localfile_plugin
from tabsdatak.conn.s3 import S3SrcConn, _plugin as _s3_plugin
from tabsdatak.conn.stream.stage.error import ManifestStageErrorCode


class StageBackend(ABC):
    """Scheme-agnostic view of one stage backend (the populated slot of
    a ``ManifestStageConn``)."""

    @property
    @abstractmethod
    def is_object_store(self) -> bool:
        """``True`` for cloud object stores (s3 / az / gcs), where a
        directory is only a key prefix; ``False`` for a local-filesystem
        stage with real directories."""

    @abstractmethod
    def build_location(self, rel: str) -> _transporter.TransporterLocation:
        """Resolve a stage-root-relative path into a transporter
        location."""

    @abstractmethod
    def credential_env(self) -> dict[str, str] | None:
        """The transporter binary's credential env for this backend, or
        ``None`` when none is required (local)."""


class _LocalStageBackend(StageBackend):
    """Local-filesystem stage. The localfile connector's
    ``build_location`` applies ``base_path`` itself, so paths pass
    through verbatim, and no credentials are needed."""

    def __init__(self, conn: LocalFileSrcConn) -> None:
        self._conn = conn

    @property
    def is_object_store(self) -> bool:
        return False

    def build_location(self, rel: str) -> _transporter.TransporterLocation:
        return _localfile_plugin.build_location(self._conn, rel)

    def credential_env(self) -> dict[str, str] | None:
        return None


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

    def build_location(self, rel: str) -> _transporter.TransporterLocation:
        return self._plugin.build_location(self._conn, self._conn.full_path(rel))

    def credential_env(self) -> dict[str, str] | None:
        return self._plugin.run_env(self._conn)


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
