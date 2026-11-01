#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

from tabsdatak.common._transporter.handler.handler1 import (
    Endpoint,
    TransporterCopyRequest,
    TransporterHandler1,
    endpoint_from_location,
    local_endpoint,
)
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    AnyRequest,
    Location,
    Request,
)
from tabsdatak.server._tdrunner.function._commons.transporter.transporter import (
    Transporter,
)


class Transporter1(Transporter):
    handler: TransporterHandler1

    def __init__(self) -> None:
        self.handler = TransporterHandler1()

    @staticmethod
    def endpoint(location: Location) -> Endpoint:
        return endpoint_from_location(location.uri, location.env_prefix)

    def bundle_fetch_request(
        self,
        request: Request | AnyRequest,
        target: Path,
    ) -> TransporterCopyRequest:
        return self.handler.make_request([
            (self.endpoint(request.info.function_bundle), local_endpoint(target))
        ])

    def upload_landing_request(
        self,
        request: Request,
        landing_files: list[tuple[int, Path]],
    ) -> TransporterCopyRequest:
        return self.handler.make_request([
            (local_endpoint(source), self.endpoint(request.info.landing_file(index)))
            for index, source in landing_files
        ])

    def write(self, copy_request: TransporterCopyRequest, path: Path) -> None:
        self.handler.write_request(copy_request, path)

    def run(self, request_file: Path, result_file: Path) -> int:
        return self.handler.run(request_file, result_file)
