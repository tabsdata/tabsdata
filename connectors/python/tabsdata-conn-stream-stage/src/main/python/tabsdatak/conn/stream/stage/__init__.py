#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, cast

from tabsdatak.api import Src
from tabsdatak.common._fileformat.taxonomy import DestFileFormat
from tabsdatak.common._transporter.facade import make_handler
from tabsdatak.common._transporter.handler.handler import TransporterHandler
from tabsdatak.common._transporter.invoker.transporter1 import (
    TransporterBinaryNotFoundError,
    TransporterReportError,
    TransporterRunError,
)
from tabsdatak.conn.azureblob import AzureBlobSrcConn
from tabsdatak.conn.common.file import _input, _output
from tabsdatak.conn.gcs import GCSSrcConn
from tabsdatak.conn.localfile import LocalFileSrcConn
from tabsdatak.conn.s3 import S3SrcConn
from tabsdatak.conn.stream.stage._backend import StageBackend, backend_for_conn
from tabsdatak.conn.stream.stage.error import ManifestStageErrorCode
from tabsdatak.error import ApiException
from tabsdatak.spi import (
    Conn,
    SrcDef,
    SrcPlugin,
    SrcPluginCtx,
    StreamStageSrc,
    StreamStageSrcPlugin,
    TableFileInput,
    TableFileSpec,
    TableMode,
    _StreamStageSrcConn,
    to_table_file_inputs,
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
# ``conn.base_path`` (typically ``files/<TS>/<name>.parquet``, one
# subdir per manifest).
_STREAM_MANIFEST_FILE_WILDCARD = "manifests/stream-manifest-*.json"

# Per-flow cursor for the manifest wildcard. Dot-separated, hyphenated
# segments only (no underscores) so it matches the context-key regex, and
# kept out of the file connector's reserved ``file-lastmod.*`` namespace.
_MANIFEST_LASTMOD_KEY = "stream-manifest.lastmod"

# Baseline handed to the transporter so last-modified tracking activates on
# the first run: epoch means every manifest ever written is a candidate, and
# the returned cursor then advances one manifest at a time.
_MANIFEST_INITIAL_LASTMOD = "1970-01-01T00:00:00.000000+00:00"


class ManifestStageSrcPlugin(SrcPlugin[ManifestStageConn, ManifestStageSrc]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: ManifestStageConn,
        src: ManifestStageSrc,
    ) -> list[list[TableFileInput]]:
        backend = backend_for_conn(conn)
        handler = make_handler()

        try:
            env = backend.credential_env(handler)
        except (ApiException, TypeError, ValueError) as e:
            raise ManifestStageErrorCode.MANIFEST_STAGE_2.exception(
                cause=e, reason=str(e)
            )

        manifest_path = self._fetch_manifest(ctx, backend, handler, env)
        if manifest_path is None:
            # No new manifest matched this trigger; nothing to materialise.
            logger.info("manifest-stage: no new manifest matched; producing 0 slots")
            return []

        slots = self._parse_manifest(manifest_path)
        return to_table_file_inputs(
            self._fetch_data_files(ctx, backend, handler, env, manifest_path, slots)
        )

    # -- steps ------------------------------------------------------

    def _fetch_manifest(
        self,
        ctx: SrcPluginCtx,
        backend: StageBackend,
        handler: TransporterHandler,
        env: dict[str, str] | None,
    ) -> Path | None:
        manifest_dir = ctx.work_dir / "_manifest"
        manifest_dir.mkdir(parents=True, exist_ok=True)
        source = backend.build_location(_STREAM_MANIFEST_FILE_WILDCARD, handler)

        prior = cast(str | None, ctx.get_attr(_MANIFEST_LASTMOD_KEY))
        try:
            report = _input.wildcard_copy_one(
                handler=handler,
                source=source,
                target_dir=manifest_dir,
                initial_lastmod=_MANIFEST_INITIAL_LASTMOD,
                lastmod_info=prior,
                max_files=1,
                env=env,
            )
        except TransporterBinaryNotFoundError as e:
            raise ManifestStageErrorCode.MANIFEST_STAGE_3.exception(cause=e)
        except TransporterRunError as e:
            raise ManifestStageErrorCode.MANIFEST_STAGE_4.exception(
                cause=e, pattern=_STREAM_MANIFEST_FILE_WILDCARD
            )
        except TransporterReportError as e:
            raise ManifestStageErrorCode.MANIFEST_STAGE_5.exception(
                cause=e, pattern=_STREAM_MANIFEST_FILE_WILDCARD
            )

        # Advance the cursor so the next trigger picks up the next manifest.
        # ``None`` means nothing was observed; leave the prior cursor in place.
        if report.lastmod_info is not None:
            ctx.set_attr(_MANIFEST_LASTMOD_KEY, report.lastmod_info)

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
        backend: StageBackend,
        handler: TransporterHandler,
        env: dict[str, str] | None,
        manifest_path: Path,
        slots: list[list[str]],
    ) -> list[list[TableFileSpec]]:
        pairs: list[tuple[Any, Any]] = []
        paths: list[str] = []
        results: list[list[TableFileSpec]] = []
        for slot_idx, slot in enumerate(slots):
            slot_dir = ctx.work_dir / f"slot{slot_idx}"
            slot_dir.mkdir(parents=True, exist_ok=True)
            slot_targets: list[TableFileSpec] = []
            for file_idx, rel in enumerate(slot):
                source = backend.build_location(rel, handler)
                name = Path(rel).name or f"file{file_idx}"
                target_path = slot_dir / f"{file_idx:04d}-{name}"
                target = handler.local_path_location(target_path)
                pairs.append((source, target))
                paths.append(str(target_path))
                slot_targets.append(target_path)
            results.append(slot_targets)

        if pairs:
            try:
                _output.copy_one(
                    handler=handler,
                    source_target_pairs=pairs,
                    work_dir=ctx.work_dir,
                    file_format=DestFileFormat.PARQUET,
                    paths=paths,
                    env=env,
                )
            except TransporterBinaryNotFoundError as e:
                raise ManifestStageErrorCode.MANIFEST_STAGE_3.exception(cause=e)
            except TransporterRunError as e:
                raise ManifestStageErrorCode.MANIFEST_STAGE_8.exception(
                    cause=e, path=str(manifest_path)
                )
            except TransporterReportError as e:
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
    type_=".stream-stage",
    system="Tabsdata",
    src_version="v1",
    src=StreamStageSrc,
    table_mode=TableMode.ARRAY,
    cardinality=lambda s: -1,
    plugin=StreamStageSrcPlugin,
    explorer=None,
)
