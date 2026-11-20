#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from collections.abc import Iterable
from enum import Enum
from pathlib import Path
from typing import Any, Protocol

import polars as pl

from tabsdatak._api import warn_legacy_stage
from tabsdatak.api import BasicDictSpec, StreamThresholds
from tabsdatak.server._tdrunner.function._commons.context.request import (
    FromFlowRequestCtx,
)
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    StreamerInfo,
)
from tabsdatak.server._tdrunner.function._commons.document.state.stream import (
    StreamState,
)
from tabsdatak.server._tdrunner.function._commons.manifests.stage import (
    StageManifest,
)
from tabsdatak.server._tdrunner.function._commons.storage.stage import Stage
from tabsdatak.spi import (
    LazyFrameInput,
    StageFileStats,
    StreamSrcPlugin,
    StreamSrcPluginCtx,
    StreamStateSpec,
)

logger = logging.getLogger(__name__)


class StageForm(Enum):
    FILES = "local Parquet Files"
    FRAMES = "LazyFrameInputs"


class StageFormError(TypeError):
    locked: StageForm
    attempted: StageForm

    def __init__(self, locked: StageForm, attempted: StageForm) -> None:
        super().__init__(
            f"Cannot stage {attempted.value} after staging {locked.value}; a "
            f"plugin stages either {StageForm.FILES.value} or {StageForm.FRAMES.value}"
        )
        self.locked = locked
        self.attempted = attempted


class Batch(Protocol):
    @property
    def pending(self) -> list[list[StageFileStats]]: ...

    def upload(self, tables_files: list[list[Path]]) -> None: ...

    def stage(
        self,
        tables_files: list[Iterable[LazyFrameInput] | None],
    ) -> list[list[StageFileStats]]: ...

    def scan(self, stats: StageFileStats) -> pl.LazyFrame: ...

    def unstage(self, files: Iterable[StageFileStats] | None = None) -> None: ...

    def publish(self) -> StageManifest: ...


class StreamSourceContext(FromFlowRequestCtx, StreamSrcPluginCtx):
    staging: Stage
    batch: Batch
    plugin: type[StreamSrcPlugin]
    stream_thresholds: StreamThresholds
    tabsdata_version: str
    running: bool
    form: StageForm | None
    view: StreamState | None

    def __init__(
        self,
        *,
        work_folder: Path,
        project_id: str,
        project: str,
        collection_id: str,
        collection: str,
        function_id: str,
        function: str,
        function_version_id: str,
        staging: Stage,
        batch: Batch,
        plugin: type[StreamSrcPlugin],
        thresholds: StreamThresholds,
        tabsdata_version: str,
    ) -> None:
        super().__init__(
            work_folder=work_folder,
            project_id=project_id,
            project=project,
            collection_id=collection_id,
            collection=collection,
            function_id=function_id,
            function=function,
            function_version_id=function_version_id,
        )
        self.staging = staging
        self.batch = batch
        self.plugin = plugin
        self.stream_thresholds = thresholds
        self.tabsdata_version = tabsdata_version
        self.running = True
        self.form = None
        self.view = StreamState.load(staging)

    @classmethod
    def from_info(
        cls,
        work_folder: Path,
        info: StreamerInfo,
        *,
        staging: Stage,
        batch: Batch,
        plugin: type[StreamSrcPlugin],
        thresholds: StreamThresholds,
        tabsdata_version: str,
    ) -> StreamSourceContext:
        return cls(
            work_folder=work_folder,
            project_id=str(info.project_id),
            project=info.project,
            collection_id=str(info.collection_id),
            collection=info.collection,
            function_id=str(info.function_id),
            function=info.function,
            function_version_id=str(info.function_version_id),
            staging=staging,
            batch=batch,
            plugin=plugin,
            thresholds=thresholds,
            tabsdata_version=tabsdata_version,
        )

    @property
    def active(self) -> bool:
        return self.running

    @property
    def first_stage(self) -> bool:
        return self.view is None

    @property
    def stream_state(self) -> BasicDictSpec | None:
        if self.view is None:
            return None
        return self.view.state

    @property
    def thresholds(self) -> StreamThresholds:
        return self.stream_thresholds

    @property
    def pending(self) -> list[list[StageFileStats]]:
        return self.batch.pending

    def stage(
        self,
        tables_files: list[list[Path]] | list[Iterable[LazyFrameInput] | None] | None,
        new_state: StreamStateSpec | None = None,
    ) -> list[list[StageFileStats]] | None:
        if new_state is not None and not isinstance(new_state, dict):
            if not callable(new_state):
                raise TypeError(
                    f"Cannot commit a {type(new_state).__name__} as the new "
                    f"state; commit a dict, or a callable when the external "
                    f"system owns the offset"
                )
        if tables_files is None:
            if new_state is None:
                raise TypeError(
                    "Cannot stage without tables to send or a new state to "
                    "publish them with"
                )
            self.lock(StageForm.FRAMES)
            self.publish(new_state)
            return None
        if legacy(tables_files):
            if new_state is None:
                raise TypeError(
                    "Cannot stage parquet files without a new state; send "
                    "LazyFrameInputs to send without publishing"
                )
            self.lock(StageForm.FILES)
            self.batch.upload(tables_files)
            self.publish(new_state)
            return None
        self.lock(StageForm.FRAMES)
        sent = self.batch.stage(tables_files)
        if new_state is not None:
            self.publish(new_state)
        return sent

    def lock(self, form: StageForm) -> None:
        locked = self.form
        if locked is None:
            self.form = form
            if form is StageForm.FILES:
                logger.warning(warn_legacy_stage(self.plugin))
            return
        if locked is not form:
            raise StageFormError(locked, form)

    def publish(self, new_state: StreamStateSpec) -> None:
        manifest = self.batch.publish()
        logger.info(
            f"Staged manifest {manifest.filename} with files per table "
            f"{[len(files) for files in manifest.tables]}"
        )
        if isinstance(new_state, dict):
            self.save(dict(new_state), manifest.batch)
        else:
            # The external system commits first: if it fails, no marker is
            # saved and the next start seeds again (at least once).
            kept = new_state()
            if kept is not None and not isinstance(kept, dict):
                raise TypeError(
                    f"Cannot keep a {type(kept).__name__} as the state besides "
                    f"the offset the external system owns; return a dict, or None"
                )
            self.save(dict(kept or {}), manifest.batch)

    def scan(self, file: StageFileStats) -> pl.LazyFrame:
        self.lock(StageForm.FRAMES)
        return self.batch.scan(file)

    def unstage(self, files: Iterable[StageFileStats] | None = None) -> None:
        if files is not None:
            self.lock(StageForm.FRAMES)
        self.batch.unstage(files)

    def checkpoint(self, new_state: BasicDictSpec) -> None:
        if self.view is None:
            external_id = None
        else:
            external_id = self.view.external_id
        self.save(dict(new_state), external_id)

    def deactivate(self) -> None:
        self.running = False

    def save(self, state: dict[str, Any], external_id: str | None) -> None:
        view = StreamState.of(
            state,
            external_id=external_id,
            tabsdata_version=self.tabsdata_version,
        )
        view.save(self.staging)
        self.view = view
        logger.debug(f"Saved the stream state with {len(state)} key(s)")


def legacy(tables_files: list[Any]) -> bool:
    if not all(isinstance(files, list) for files in tables_files):
        return False
    files = [file for entry in tables_files for file in entry]
    return bool(files) and all(isinstance(file, Path) for file in files)
