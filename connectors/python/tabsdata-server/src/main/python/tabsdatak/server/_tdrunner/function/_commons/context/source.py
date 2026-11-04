#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path
from typing import Self

from tabsdatak.common._storage.scratch import ScratchFiles
from tabsdatak.server._tdrunner.function._commons.context.request import (
    FromRequestCtx,
)
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Info,
)
from tabsdatak.spi import SrcPluginCtx

PLUGIN_FOLDER = "plugin"


class SourceContext(FromRequestCtx, SrcPluginCtx):
    _scratch_files: ScratchFiles | None = None

    @classmethod
    def from_info(cls, work_folder: Path, info: Info) -> Self:
        context = super().from_info(work_folder, info)
        root = f"{info.function_scratch.uri.rstrip('/')}/{PLUGIN_FOLDER}"
        options = info.function_scratch.mount_declaration().storage_options(root)
        context._scratch_files = ScratchFiles(root, options)
        return context

    @property
    def _scratch(self) -> ScratchFiles:
        if self._scratch_files is None:
            raise NotImplementedError("This context has no storage scratch")
        return self._scratch_files
