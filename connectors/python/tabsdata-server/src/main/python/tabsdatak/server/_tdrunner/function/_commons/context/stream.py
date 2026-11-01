#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

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
from tabsdatak.spi import StreamSrcPluginCtx, StreamStateSpec

logger = logging.getLogger(__name__)


class StreamSourceContext(FromFlowRequestCtx, StreamSrcPluginCtx):
    staging: Stage
    stream_thresholds: StreamThresholds
    tabsdata_version: str
    running: bool
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
        self.stream_thresholds = thresholds
        self.tabsdata_version = tabsdata_version
        self.running = True
        self.view = StreamState.load(staging)

    @classmethod
    def from_info(
        cls,
        work_folder: Path,
        info: StreamerInfo,
        *,
        staging: Stage,
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

    def stage(
        self,
        tables_files: list[list[Path]],
        new_state: StreamStateSpec,
    ) -> None:
        manifest = StageManifest.now()
        staged: list[list[str]] = []
        for position, files in enumerate(tables_files):
            names: list[str] = []
            for index, file in enumerate(files):
                if not file.is_file():
                    raise FileNotFoundError(
                        f"File {index} of table {position} ({file}) does not exist"
                    )
                name = manifest.file(file.name)
                self.staging.upload(file, name)
                file.unlink()
                names.append(name)
            staged.append(names)
        # Written after its files, so a visible manifest never names data not
        # yet staged.
        manifest.with_tables(staged).write(self.staging)
        logger.info(
            f"Staged manifest {manifest.filename} with files per table "
            f"{[len(names) for names in staged]}"
        )
        if isinstance(new_state, dict):
            self.save(dict(new_state), manifest.batch)
        else:
            # The external system commits first: if it fails, no marker is
            # saved and the next start seeds again (at least once).
            new_state()
            self.save({}, manifest.batch)

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
