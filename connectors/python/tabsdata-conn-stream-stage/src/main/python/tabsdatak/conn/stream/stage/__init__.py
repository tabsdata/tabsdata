#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Callable, cast

from tabsdatak.api import Conn, Src
from tabsdatak.conn.azureblob import AzureBlobSrcConn, _plugin as _azureblob_plugin
from tabsdatak.conn.common.file import _input, _output, _transporter
from tabsdatak.conn.gcs import GCSSrcConn, _plugin as _gcs_plugin
from tabsdatak.conn.localfile import LocalFileSrcConn, _plugin as _localfile_plugin
from tabsdatak.conn.s3 import S3SrcConn, _plugin as _s3_plugin
from tabsdatak.conn.stream.stage.error import ManifestStageErrorCode
from tabsdatak.error import ApiException
from tabsdatak.spi import (
    SrcDef,
    SrcPlugin,
    SrcPluginCtx,
    StreamStageSrc,
    StreamStageSrcPlugin,
    TableFileSpec,
    TableMode,
    _StreamStageSrcConn,
)

logger = logging.getLogger(__name__)


class ManifestStageConn(Conn):
    local: LocalFileSrcConn | None
    s3: S3SrcConn | None
    az: AzureBlobSrcConn | None
    gcs: GCSSrcConn | None


class ManifestStageSrc(Src):
    pass


# Manifests live under the conn's ``manifests/`` subdir (the layout
# the streamer pod writes; see
# ``tabsdata-server/_lib/stage_layout.py`` and
# ``_lib/stage_manifest.py``). The wildcard is relative to
# ``conn.base_path``, which by convention is the stage root, so this
# globs ``<stage_root>/manifests/stream-manifest-*.json``. The file
# paths inside each manifest stay relative to the same
# ``conn.base_path`` (typically ``files/<name>.parquet``).
_STREAM_MANIFEST_FILE_WILDCARD = "manifests/stream-manifest-*.json"

# Per-flow cursor for the manifest wildcard. Dot-separated, hyphenated
# segments only (no underscores) so it matches the context-key regex, and
# kept out of the file connector's reserved ``file-lastmod.*`` namespace.
_MANIFEST_LASTMOD_KEY = "stream-manifest.lastmod"

# Baseline handed to the transporter so last-modified tracking activates on
# the first run: epoch means every manifest ever written is a candidate, and
# the returned cursor then advances one manifest at a time.
_MANIFEST_INITIAL_LASTMOD = "1970-01-01T00:00:00.000000+00:00"


# A backend builds a transporter source location from a path relative to the
# conn's ``base_path``, and (optionally) the credential env for the binary.
_LocationBuilder = Callable[[str], _transporter.TransporterLocation]
_EnvBuilder = Callable[[], dict[str, str]]


def _active_backend(
    conn: ManifestStageConn,
) -> tuple[_LocationBuilder, _EnvBuilder | None]:
    """Resolve the single populated backend on ``conn`` into a
    location builder + (optional) credential-env builder.

    The cloud connectors' ``build_location`` does not itself apply
    ``base_path``, so we resolve paths through ``conn.full_path`` here;
    the local connector's ``build_location`` applies it internally, so
    its paths are passed through verbatim.
    """
    candidates = [
        (conn.local, _localfile_plugin, True),
        (conn.s3, _s3_plugin, False),
        (conn.az, _azureblob_plugin, False),
        (conn.gcs, _gcs_plugin, False),
    ]
    populated = [(c, mod, is_local) for c, mod, is_local in candidates if c is not None]
    if len(populated) != 1:
        raise ManifestStageErrorCode.MANIFEST_STAGE_1.exception(count=len(populated))
    backend_conn, mod, is_local = populated[0]

    def build_location(rel: str) -> _transporter.TransporterLocation:
        if is_local:
            return mod.build_location(backend_conn, rel)
        return mod.build_location(backend_conn, backend_conn.full_path(rel))

    run_env = getattr(mod, "run_env", None)
    env_builder: _EnvBuilder | None = None
    if run_env is not None:

        def env_builder() -> dict[str, str]:
            return run_env(backend_conn)

    return build_location, env_builder


class ManifestStageSrcPlugin(SrcPlugin[ManifestStageConn, ManifestStageSrc]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: ManifestStageConn,
        src: ManifestStageSrc,
    ) -> list[list[TableFileSpec]]:
        build_location, env_builder = _active_backend(conn)

        env: dict[str, str] | None = None
        if env_builder is not None:
            try:
                env = env_builder()
            except (ApiException, TypeError, ValueError) as e:
                raise ManifestStageErrorCode.MANIFEST_STAGE_2.exception(
                    cause=e, reason=str(e)
                )

        manifest_path = self._fetch_manifest(ctx, build_location, env)
        if manifest_path is None:
            # No new manifest matched this trigger; nothing to materialise.
            logger.info("manifest-stage: no new manifest matched; producing 0 slots")
            return []

        slots = self._parse_manifest(manifest_path)
        return self._fetch_data_files(ctx, build_location, env, manifest_path, slots)

    # -- steps ------------------------------------------------------

    def _fetch_manifest(
        self,
        ctx: SrcPluginCtx,
        build_location: _LocationBuilder,
        env: dict[str, str] | None,
    ) -> Path | None:
        manifest_dir = ctx.work_dir / "_manifest"
        manifest_dir.mkdir(parents=True, exist_ok=True)
        source = build_location(_STREAM_MANIFEST_FILE_WILDCARD)

        prior = cast(str | None, ctx.get_attr(_MANIFEST_LASTMOD_KEY))
        try:
            report = _input.wildcard_copy_one(
                source=source,
                target_dir=manifest_dir,
                initial_lastmod=_MANIFEST_INITIAL_LASTMOD,
                lastmod_info=prior,
                max_files=1,
                env=env,
            )
        except _transporter.TransporterBinaryNotFoundError as e:
            raise ManifestStageErrorCode.MANIFEST_STAGE_3.exception(cause=e)
        except _transporter.TransporterRunError as e:
            raise ManifestStageErrorCode.MANIFEST_STAGE_4.exception(
                cause=e, pattern=_STREAM_MANIFEST_FILE_WILDCARD
            )
        except _transporter.TransporterReportError as e:
            raise ManifestStageErrorCode.MANIFEST_STAGE_5.exception(
                cause=e, pattern=_STREAM_MANIFEST_FILE_WILDCARD
            )

        # Advance the cursor so the next trigger picks up the next manifest.
        # ``None`` means nothing was observed; leave the prior cursor in place.
        if report.lastmod_info is not None:
            ctx.store_attr(_MANIFEST_LASTMOD_KEY, report.lastmod_info)

        targets = [f["to"] for f in report.files if isinstance(f, dict) and f.get("to")]
        if not targets:
            return None
        manifest_path = _local_path(cast(str, targets[0]))
        logger.info("manifest-stage: fetched manifest %s", manifest_path)
        return manifest_path

    def _parse_manifest(self, manifest_path: Path) -> list[list[str]]:
        try:
            raw: Any = json.loads(manifest_path.read_text())
        except (OSError, ValueError) as e:
            raise ManifestStageErrorCode.MANIFEST_STAGE_6.exception(
                cause=e, path=str(manifest_path)
            )
        if not isinstance(raw, list) or not all(
            isinstance(slot, list) and all(isinstance(p, str) for p in slot)
            for slot in raw
        ):
            raise ManifestStageErrorCode.MANIFEST_STAGE_7.exception(
                path=str(manifest_path)
            )
        return raw

    def _fetch_data_files(
        self,
        ctx: SrcPluginCtx,
        build_location: _LocationBuilder,
        env: dict[str, str] | None,
        manifest_path: Path,
        slots: list[list[str]],
    ) -> list[list[TableFileSpec]]:
        pairs: list[
            tuple[_transporter.TransporterLocation, _transporter.TransporterLocation]
        ] = []
        results: list[list[TableFileSpec]] = []
        for slot_idx, slot in enumerate(slots):
            slot_dir = ctx.work_dir / f"slot{slot_idx}"
            slot_dir.mkdir(parents=True, exist_ok=True)
            slot_targets: list[TableFileSpec] = []
            for file_idx, rel in enumerate(slot):
                source = build_location(rel)
                name = Path(rel).name or f"file{file_idx}"
                target_path = slot_dir / f"{file_idx:04d}-{name}"
                target = _transporter.TransporterLocalFile(
                    _transporter.path_to_file_uri(target_path)
                )
                pairs.append((source, target))
                slot_targets.append(target_path)
            results.append(slot_targets)

        if pairs:
            try:
                _output.copy_one(
                    source_target_pairs=pairs,
                    work_dir=ctx.work_dir,
                    format=_transporter.TransporterParquetFormat(),
                    env=env,
                )
            except _transporter.TransporterBinaryNotFoundError as e:
                raise ManifestStageErrorCode.MANIFEST_STAGE_3.exception(cause=e)
            except _transporter.TransporterRunError as e:
                raise ManifestStageErrorCode.MANIFEST_STAGE_8.exception(
                    cause=e, path=str(manifest_path)
                )
            except _transporter.TransporterReportError as e:
                raise ManifestStageErrorCode.MANIFEST_STAGE_9.exception(
                    cause=e, path=str(manifest_path)
                )

        logger.info(
            "manifest-stage: materialised %d slot(s) (%d file(s))",
            len(results),
            len(pairs),
        )
        return results


def _local_path(file_uri: str) -> Path:
    from urllib.parse import unquote, urlparse

    parsed = urlparse(file_uri)
    if parsed.scheme == "file":
        return Path(unquote(parsed.path))
    return Path(file_uri)


# Default stream-stage source binding. The manifest decides how many
# output slots a run yields, so ``cardinality`` returns ``-1`` -- the
# sentinel that disables the framework's produced-vs-declared slot-count
# check -- and there is no explorer. ``ARRAY`` mode because a single
# manifest slot may list more than one parquet file.
STREAM_STAGE_SRC = SrcDef(
    conn=_StreamStageSrcConn,
    src_type="streamStage",
    src_version="v1",
    src=StreamStageSrc,
    table_mode=TableMode.ARRAY,
    cardinality=lambda s: -1,
    plugin=StreamStageSrcPlugin,
    explorer=None,
)
