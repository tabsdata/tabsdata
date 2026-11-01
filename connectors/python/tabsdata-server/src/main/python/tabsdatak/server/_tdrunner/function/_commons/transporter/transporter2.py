#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

from tabsdatak.common._storage.mounts import (
    MOUNTS_KEY,
    OPTIONS_KEY,
    STORAGE_KEY,
    MountError,
    NamedMountDeclaration,
    secret_variable,
)
from tabsdatak.common._transporter.handler.handler2 import (
    TransporterHandler2,
    local_location,
)
from tabsdatak.common._transporter.invoker import transporter2
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    AnyRequest,
    Location,
    Request,
)
from tabsdatak.server._tdrunner.function._commons.transporter.transporter import (
    Transporter,
)
from tabsdatak.server._tdrunner.function._lib.errors import CommandErrorCode

ENVIRONMENT_REFERENCE = "env:"


class Transporter2(Transporter):
    handler: TransporterHandler2
    option_keys: dict[str, list[str]]

    def __init__(self, option_keys: dict[str, list[str]]) -> None:
        self.handler = TransporterHandler2()
        self.option_keys = option_keys

    @classmethod
    def of(cls, storage_configuration_file: Path) -> Transporter2:
        try:
            declarations = NamedMountDeclaration.read_all(storage_configuration_file)
        except MountError as error:
            raise CommandErrorCode.CMD_3.exception(
                cause=error,
                path=storage_configuration_file,
                detail=str(error),
            )
        return cls({
            declaration.id: list(declaration.options) for declaration in declarations
        })

    def location(self, location: Location) -> transporter2.Location:
        if location.env_prefix is None:
            return transporter2.Location(uri=location.uri)
        keys = self.option_keys.get(location.env_prefix)
        if keys is None:
            raise CommandErrorCode.CMD_4.exception(
                uri=location.uri,
                mount=location.env_prefix,
            )
        configs = {
            key: ENVIRONMENT_REFERENCE
            + secret_variable(
                STORAGE_KEY,
                MOUNTS_KEY,
                location.env_prefix,
                OPTIONS_KEY,
                key,
            )
            for key in keys
        }
        return transporter2.Location(uri=location.uri, configs=configs)

    def bundle_fetch_request(
        self,
        request: Request | AnyRequest,
        target: Path,
    ) -> transporter2.CopyRequest:
        return self.handler.make_request([
            (self.location(request.info.function_bundle), local_location(target))
        ])

    def upload_landing_request(
        self,
        request: Request,
        landing_files: list[tuple[int, Path]],
    ) -> transporter2.CopyRequest:
        return self.handler.make_request([
            (local_location(source), self.location(request.info.landing_file(index)))
            for index, source in landing_files
        ])

    def write(self, copy_request: transporter2.CopyRequest, path: Path) -> None:
        self.handler.write_request(copy_request, path)

    def run(self, request_file: Path, result_file: Path) -> int:
        return self.handler.run(request_file, result_file)
