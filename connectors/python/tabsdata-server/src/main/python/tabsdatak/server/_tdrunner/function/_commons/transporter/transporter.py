#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from tabsdatak.common._transporter.facade import TRANSPORTER_MODE
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    AnyRequest,
    Request,
)
from tabsdatak.server._tdrunner.function._lib.errors import CommandErrorCode


class Transporter(ABC):
    @staticmethod
    def of(storage_configuration_file: Path) -> Transporter:
        from tabsdatak.server._tdrunner.function._commons.transporter.transporter1 import (  # noqa: E501
            Transporter1,
        )
        from tabsdatak.server._tdrunner.function._commons.transporter.transporter2 import (  # noqa: E501
            Transporter2,
        )

        match TRANSPORTER_MODE:
            case 1:
                return Transporter1()
            case 2:
                return Transporter2.of(storage_configuration_file)
            case _:
                raise CommandErrorCode.CMD_5.exception(mode=TRANSPORTER_MODE)

    @abstractmethod
    def bundle_fetch_request(
        self,
        request: Request | AnyRequest,
        target: Path,
    ) -> Any: ...

    @abstractmethod
    def upload_landing_request(
        self,
        request: Request,
        landing_files: list[tuple[int, Path]],
    ) -> Any: ...

    @abstractmethod
    def write(self, copy_request: Any, path: Path) -> None: ...

    @abstractmethod
    def run(self, request_file: Path, result_file: Path) -> int: ...
