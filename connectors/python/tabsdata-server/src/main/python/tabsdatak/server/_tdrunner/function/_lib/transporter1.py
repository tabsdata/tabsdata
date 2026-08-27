#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from tabsdatak.common._transporter.handler.handler1 import (
    Endpoint,
    TransporterCopyRequest,
    TransporterHandler1,
    endpoint_from_location,
    local_endpoint,
)
from tabsdatak.server._tdrunner.function._lib.outputs_manifest import (
    MANIFEST_FILE_KEY,
    MANIFEST_NAME_KEY,
)
from tabsdatak.server._tdrunner.function._lib.request import (
    RAW_SUFFIX,
    AnyRequest,
    InputTable,
    InputTableVersions,
    Location,
    Request,
    input_tables,
)

logger = logging.getLogger(__name__)

HANDLER = TransporterHandler1()


class OutputTableNotInManifestError(ValueError):
    pass


def endpoint_for(location: Location) -> Endpoint:
    return endpoint_from_location(location.uri, location.env_prefix)


def localized_input_files(
    in_dir: Path, entry: InputTable | InputTableVersions
) -> list[tuple[InputTable, Path]]:
    if isinstance(entry, InputTableVersions):
        return [
            (member, in_dir / f"{member.name}.{member.version_pos}.parquet")
            for member in input_tables(entry)
        ]
    return [(entry, in_dir / f"{entry.name}.parquet")]


def build_localize_state_request(
    request: Request, in_dir: Path, state_dir: Path
) -> TransporterCopyRequest:
    pairs: list[tuple[Endpoint, Endpoint]] = []
    for entry in request.input:
        for member, path in localized_input_files(in_dir, entry):
            if member.location is None:
                continue
            pairs.append((endpoint_for(member.location), local_endpoint(path)))
    for table in request.system_input:
        if table.location is None:
            continue
        pairs.append((
            endpoint_for(table.location),
            local_endpoint(state_dir / f"{table.name}.parquet"),
        ))
    return HANDLER.make_request(pairs)


def build_bundle_fetch_request(
    request: Request | AnyRequest, target_path: Path
) -> TransporterCopyRequest:
    return HANDLER.make_request([
        (endpoint_for(request.info.function_bundle), local_endpoint(target_path))
    ])


def build_upload_state_request(
    request: Request, state_dir: Path
) -> TransporterCopyRequest:
    pairs: list[tuple[Endpoint, Endpoint]] = []
    for table in request.system_output:
        if table.location is None:
            continue
        source_path = state_dir / f"{table.name}.parquet"
        if not source_path.exists():
            continue
        pairs.append((local_endpoint(source_path), endpoint_for(table.location)))
    return HANDLER.make_request(pairs)


def build_upload_tables_request(
    request: Request, manifest_entries: list[dict[str, Any]]
) -> TransporterCopyRequest:
    files_by_name: dict[str, str | None] = {
        entry[MANIFEST_NAME_KEY]: entry.get(MANIFEST_FILE_KEY)
        for entry in manifest_entries
    }
    pairs: list[tuple[Endpoint, Endpoint]] = []
    for table in request.output:
        if table.location is None:
            continue
        if table.name not in files_by_name:
            raise OutputTableNotInManifestError(
                f"Output table {table.name!r} declared in request "
                f"has no entry in the outputs manifest."
            )
        source = files_by_name[table.name]
        if source is None:
            continue
        pairs.append((local_endpoint(Path(source)), endpoint_for(table.location)))
    return HANDLER.make_request(pairs)


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
