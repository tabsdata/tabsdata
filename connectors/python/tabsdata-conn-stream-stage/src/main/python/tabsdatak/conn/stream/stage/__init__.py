#
# Copyright 2026 Tabsdata Inc.
#

"""Stream-stage connector -- internal streaming source.

Framework plumbing for the streaming pipeline (not a user-facing
database connector). A stream source plugin stages batches of tables to
the function stage, each named by a manifest; this source picks up one
new manifest per trigger and reads the tables it lists where they are
staged.

Manifests live under `<stage_root>/manifests/` and are consumed in
last-modified order; a per-flow cursor advances one manifest per run.
"""

from __future__ import annotations

import fnmatch
import json
import logging
from collections.abc import Iterable
from datetime import datetime
from typing import Any, Protocol, cast
from urllib.parse import urlparse

import obstore

from tabsdatak.common._storage.frames import FrameLocator
from tabsdatak.common._storage.landed import LANDING_SUFFIX, LandedPair, LandedTable
from tabsdatak.common._storage.transfer import OBJECT_STORES, StorageLocation
from tabsdatak.conn.common._cursor import LastModifiedCursor
from tabsdatak.conn.stream.stage.error import ManifestStageErrorCode
from tabsdatak.spi import (
    LazyFrameInput,
    SrcDef,
    SrcPlugin,
    SrcPluginCtx,
    StreamStageSrc,
    StreamStageSrcPlugin,
    TableMode,
    _StreamStageSrcConn,
)

logger = logging.getLogger(__name__)

MANIFESTS_FOLDER = "manifests"
MANIFEST_PATTERN = "stream-manifest-*.json"
PATH_SEPARATOR = "/"

MANIFEST_CURSOR_KEY = "stream-manifest.lastmod"
MANIFEST_INITIAL_LASTMOD = datetime.fromisoformat("1970-01-01T00:00:00.000000+00:00")


class StageConnection(Protocol):
    location: StorageLocation


class ManifestStageSrcPlugin(SrcPlugin[_StreamStageSrcConn, StreamStageSrc]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: _StreamStageSrcConn,
        src: StreamStageSrc,
    ) -> list[Iterable[LazyFrameInput]]:
        location = cast(StageConnection, conn).location
        root = location.uri.rstrip(PATH_SEPARATOR)
        manifest = self.next_manifest(ctx, location, root)
        if manifest is None:
            logger.info("manifest-stage: no new manifest matched; producing 0 slots")
            return []
        tables = self.read_manifest(location, manifest)
        logger.info(
            f"manifest-stage: read manifest {manifest} with files per slot "
            f"{[len(names) for names in tables]}"
        )
        return [
            [self.table(location, root, name) for name in names] for names in tables
        ]

    @staticmethod
    def next_manifest(
        ctx: SrcPluginCtx,
        location: StorageLocation,
        root: str,
    ) -> str | None:
        pattern = f"{root}/{MANIFESTS_FOLDER}/{MANIFEST_PATTERN}"
        store = OBJECT_STORES.store(root, location.options)
        try:
            listing = obstore.list_with_delimiter(store, prefix=MANIFESTS_FOLDER)
        except Exception as error:
            raise ManifestStageErrorCode.MANIFEST_STAGE_4.exception(
                cause=error,
                pattern=pattern,
                reason=str(error),
            )
        manifests = sorted(
            (
                item
                for item in listing["objects"]
                if fnmatch.fnmatchcase(
                    item["path"].rpartition(PATH_SEPARATOR)[2],
                    MANIFEST_PATTERN,
                )
            ),
            key=lambda item: (item["last_modified"], item["path"]),
        )
        cursor = LastModifiedCursor.decode(
            cast(str | None, ctx.get_attr(MANIFEST_CURSOR_KEY)),
            MANIFEST_INITIAL_LASTMOD,
        )
        key = urlparse(pattern).path
        chosen = None
        for item in manifests:
            name = item["path"].rpartition(PATH_SEPARATOR)[2]
            if cursor.take(key, name, item["last_modified"]):
                chosen = item["path"]
                break
        ctx.set_attr(MANIFEST_CURSOR_KEY, cursor.encode())
        return chosen

    @staticmethod
    def read_manifest(location: StorageLocation, path: str) -> list[list[str]]:
        root = location.uri.rstrip(PATH_SEPARATOR)
        store = OBJECT_STORES.store(root, location.options)
        try:
            raw: Any = json.loads(bytes(obstore.get(store, path).bytes()))
        except (OSError, ValueError) as error:
            raise ManifestStageErrorCode.MANIFEST_STAGE_6.exception(
                cause=error,
                path=path,
            )
        if not isinstance(raw, list) or not all(
            isinstance(names, list) and all(isinstance(name, str) for name in names)
            for names in raw
        ):
            raise ManifestStageErrorCode.MANIFEST_STAGE_7.exception(path=path)
        return raw

    @staticmethod
    def table(location: StorageLocation, root: str, name: str) -> LazyFrameInput:
        data = FrameLocator(f"{root}/{name}", location.options)
        if not name.endswith(LANDING_SUFFIX):
            return LazyFrameInput(data.scan())
        pair = LandedPair.of(name)
        schema = FrameLocator(f"{root}/{pair.schema}", location.options)
        return LandedTable(data, schema).input()


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
    icon=None,
)
