#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from pathlib import Path

from tabsdatak.common._transporter.handler.handler1 import (
    Endpoint,
    TransporterCopyRequest,
    TransporterHandler1,
    endpoint_from_location,
    local_endpoint,
)
from tabsdatak.server._tdrunner.function._commons.document.request import (
    RAW_SUFFIX,
    AnyRequest,
    Location,
    Request,
)

logger = logging.getLogger(__name__)

HANDLER = TransporterHandler1()


def endpoint_for(location: Location) -> Endpoint:
    return endpoint_from_location(location.uri, location.env_prefix)


def build_bundle_fetch_request(
    request: Request | AnyRequest, target_path: Path
) -> TransporterCopyRequest:
    return HANDLER.make_request([
        (endpoint_for(request.info.function_bundle), local_endpoint(target_path))
    ])


def build_upload_raw_request(
    request: Request, raw_files: list[tuple[int, Path]]
) -> TransporterCopyRequest:
    raw = request.info.function_raw
    pairs: list[tuple[Endpoint, Endpoint]] = []
    if raw is not None:
        base = raw.uri.rstrip("/")
        for idx, source in raw_files:
            target = Location(
                uri=f"{base}/{idx}{RAW_SUFFIX}", env_prefix=raw.env_prefix
            )
            pairs.append((local_endpoint(source), endpoint_for(target)))
    return HANDLER.make_request(pairs)


def write_transporter_request(request: TransporterCopyRequest, path: Path) -> None:
    HANDLER.write_request(request, path)


def run_transporter(request_yaml: Path, report: Path) -> int:
    return HANDLER.run(request_yaml, report)
