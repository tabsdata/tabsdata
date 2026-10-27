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
    RAW_SUFFIX,
    AnyRequest,
    InputTable,
    InputTableVersions,
    Location,
    Request,
)
from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputManifest,
)
from tabsdatak.server._tdrunner.function._lib.errors import CommandErrorCode
from tabsdatak.server._tdrunner.function._lib.mounts import secret_env_name

logger = logging.getLogger(__name__)


class OutputTableNotInManifestError(ValueError):
    pass


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


def localized_input_files(
    in_dir: Path, entry: InputTable | InputTableVersions
) -> list[tuple[InputTable, Path]]:
    if isinstance(entry, InputTableVersions):
        return [
            (member, in_dir / f"{member.table}.{member.version_pos}.parquet")
            for member in entry.tables
        ]
    return [(entry, in_dir / f"{entry.table}.parquet")]


def build_localize_state_request(
    request: Request,
    in_dir: Path,
    state_dir: Path,
    option_keys: dict[str, list[str]],
) -> transporter2.CopyRequest:
    handler = ServerTransporterHandler2(option_keys)
    pairs: list[tuple[transporter2.Location, transporter2.Location]] = []
    for entry in request.input:
        for member, path in localized_input_files(in_dir, entry):
            if member.location is None:
                continue
            pairs.append((handler.location_for(member.location), local_location(path)))
    for table in request.system_input:
        if table.location is None:
            continue
        pairs.append((
            handler.location_for(table.location),
            local_location(state_dir / f"{table.table}.parquet"),
        ))
    return handler.make_request(pairs)


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


def build_upload_state_request(
    request: Request,
    state_dir: Path,
    option_keys: dict[str, list[str]],
) -> transporter2.CopyRequest:
    handler = ServerTransporterHandler2(option_keys)
    pairs: list[tuple[transporter2.Location, transporter2.Location]] = []
    for table in request.system_output:
        if table.location is None:
            continue
        source_path = state_dir / f"{table.table}.parquet"
        if not source_path.exists():
            continue
        pairs.append((
            local_location(source_path),
            handler.location_for(table.location),
        ))
    return handler.make_request(pairs)


def build_upload_tables_request(
    request: Request,
    manifest: OutputManifest,
    option_keys: dict[str, list[str]],
) -> transporter2.CopyRequest:
    handler = ServerTransporterHandler2(option_keys)
    files_by_name: dict[str, str | None] = {
        entry.name: entry.file for entry in manifest.outputs
    }
    pairs: list[tuple[transporter2.Location, transporter2.Location]] = []
    for table in request.output:
        if table.location is None:
            continue
        if table.table not in files_by_name:
            raise OutputTableNotInManifestError(
                f"Output table {table.table!r} declared in request "
                f"has no entry in the outputs manifest."
            )
        source = files_by_name[table.table]
        if source is None:
            continue
        pairs.append((
            local_location(Path(source)),
            handler.location_for(table.location),
        ))
    return handler.make_request(pairs)


def build_upload_raw_request(
    request: Request,
    raw_files: list[tuple[int, Path]],
    option_keys: dict[str, list[str]],
) -> transporter2.CopyRequest:
    handler = ServerTransporterHandler2(option_keys)
    raw = request.info.function_raw
    pairs: list[tuple[transporter2.Location, transporter2.Location]] = []
    if raw is not None:
        base = raw.uri.rstrip("/")
        for idx, source in raw_files:
            target = Location(
                uri=f"{base}/{idx}{RAW_SUFFIX}", env_prefix=raw.env_prefix
            )
            pairs.append((local_location(source), handler.location_for(target)))
    return handler.make_request(pairs)


def write_transporter_request(request: transporter2.CopyRequest, path: Path) -> None:
    ServerTransporterHandler2({}).write_request(request, path)


def run_transporter(request_yaml: Path, response_yaml: Path) -> int:
    return ServerTransporterHandler2({}).run(request_yaml, response_yaml)
