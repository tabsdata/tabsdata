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
    LANDING_SUFFIX,
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


def build_upload_landing_request(
    request: Request, landing_files: list[tuple[int, Path]]
) -> TransporterCopyRequest:
    landing = request.info.function_landing
    pairs: list[tuple[Endpoint, Endpoint]] = []
    if landing is not None:
        base = landing.uri.rstrip("/")
        for idx, source in landing_files:
            target = Location(
                uri=f"{base}/{idx}{LANDING_SUFFIX}", env_prefix=landing.env_prefix
            )
            pairs.append((local_endpoint(source), endpoint_for(target)))
    return HANDLER.make_request(pairs)


def write_transporter_request(request: TransporterCopyRequest, path: Path) -> None:
    HANDLER.write_request(request, path)


def run_transporter(request_yaml: Path, report: Path) -> int:
    return HANDLER.run(request_yaml, report)
