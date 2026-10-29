#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from pathlib import Path

from tabsdatak.common._transporter.handler.handler2 import (
    TransporterHandler2,
    local_location,
)
from tabsdatak.common._transporter.invoker import transporter2
from tabsdatak.server._tdrunner.function._commons.document.request import (
    AnyRequest,
    Location,
    Request,
)
from tabsdatak.server._tdrunner.function._lib.errors import CommandErrorCode
from tabsdatak.server._tdrunner.function._lib.mounts import secret_env_name

logger = logging.getLogger(__name__)


class ServerTransporterHandler2(TransporterHandler2):
    def __init__(self, option_keys: dict[str, list[str]]):
        self.option_keys = option_keys

    def location_for(self, location: Location) -> transporter2.Location:
        if location.env_prefix is None:
            return transporter2.Location(uri=location.uri)
        keys = self.option_keys.get(location.env_prefix)
        if keys is None:
            raise CommandErrorCode.CMD_4.exception(
                uri=location.uri, mount=location.env_prefix
            )
        configs = {
            key: f"env:{secret_env_name(location.env_prefix, key)}" for key in keys
        }
        return transporter2.Location(uri=location.uri, configs=configs)


def build_bundle_fetch_request(
    request: Request | AnyRequest,
    target_path: Path,
    option_keys: dict[str, list[str]],
) -> transporter2.CopyRequest:
    handler = ServerTransporterHandler2(option_keys)
    return handler.make_request([
        (
            handler.location_for(request.info.function_bundle),
            local_location(target_path),
        )
    ])


def build_upload_landing_request(
    request: Request,
    landing_files: list[tuple[int, Path]],
    option_keys: dict[str, list[str]],
) -> transporter2.CopyRequest:
    handler = ServerTransporterHandler2(option_keys)
    pairs: list[tuple[transporter2.Location, transporter2.Location]] = [
        (local_location(source), handler.location_for(request.info.landing_file(idx)))
        for idx, source in landing_files
    ]
    return handler.make_request(pairs)


def write_transporter_request(request: transporter2.CopyRequest, path: Path) -> None:
    ServerTransporterHandler2({}).write_request(request, path)


def run_transporter(request_yaml: Path, response_yaml: Path) -> int:
    return ServerTransporterHandler2({}).run(request_yaml, response_yaml)
