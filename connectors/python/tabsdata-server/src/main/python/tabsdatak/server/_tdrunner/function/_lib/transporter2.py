#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import subprocess
import threading
from pathlib import Path
from typing import IO, Sequence

import yaml

from tabsdatak.common._interpreter.python.error import BinaryNotFoundError
from tabsdatak.common._interpreter.python.venv import PythonEnvironment
from tabsdatak.common._transporter import _transporter2
from tabsdatak.server._tdrunner.function._lib.errors import CommandErrorCode
from tabsdatak.server._tdrunner.function._lib.mounts import secret_env_name
from tabsdatak.server._tdrunner.function._lib.outputs_manifest import (
    MANIFEST_FILE_KEY,
    MANIFEST_NAME_KEY,
)
from tabsdatak.server._tdrunner.function._lib.request import (
    AnyRequest,
    InputTable,
    InputTableVersions,
    Location,
    Request,
    input_tables,
)

logger = logging.getLogger(__name__)

TRANSPORTER_BINARY_NAME = "td_transporter2"


class OutputTableNotInManifestError(ValueError):
    pass


class TransporterNotFoundError(FileNotFoundError):
    pass


def location_for(
    location: Location, option_keys: dict[str, list[str]]
) -> _transporter2.Location:
    if location.env_prefix is None:
        return _transporter2.Location(uri=location.uri)
    keys = option_keys.get(location.env_prefix)
    if keys is None:
        raise CommandErrorCode.CMD_4.exception(
            uri=location.uri, mount=location.env_prefix
        )
    configs = {key: f"env:{secret_env_name(location.env_prefix, key)}" for key in keys}
    return _transporter2.Location(uri=location.uri, configs=configs)


def local_location(path: Path) -> _transporter2.Location:
    return _transporter2.Location(uri=path.absolute().as_uri())


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
    request: Request,
    in_dir: Path,
    state_dir: Path,
    option_keys: dict[str, list[str]],
) -> _transporter2.CopyRequest:
    pairs: list[_transporter2.CopyPair] = []
    for entry in request.input:
        for member, path in localized_input_files(in_dir, entry):
            if member.location is None:
                continue
            pairs.append(
                _transporter2.CopyPair(
                    from_=location_for(member.location, option_keys),
                    to=local_location(path),
                )
            )
    for table in request.system_input:
        if table.location is None:
            continue
        pairs.append(
            _transporter2.CopyPair(
                from_=location_for(table.location, option_keys),
                to=local_location(state_dir / f"{table.name}.parquet"),
            )
        )
    return _transporter2.CopyRequest(pairs=pairs)


def build_bundle_fetch_request(
    request: Request | AnyRequest,
    target_path: Path,
    option_keys: dict[str, list[str]],
) -> _transporter2.CopyRequest:
    return _transporter2.CopyRequest(
        pairs=[
            _transporter2.CopyPair(
                from_=location_for(request.info.function_bundle, option_keys),
                to=local_location(target_path),
            )
        ]
    )


def build_upload_state_request(
    request: Request,
    state_dir: Path,
    option_keys: dict[str, list[str]],
) -> _transporter2.CopyRequest:
    pairs: list[_transporter2.CopyPair] = []
    for table in request.system_output:
        if table.location is None:
            continue
        source_path = state_dir / f"{table.name}.parquet"
        if not source_path.exists():
            continue
        pairs.append(
            _transporter2.CopyPair(
                from_=local_location(source_path),
                to=location_for(table.location, option_keys),
            )
        )
    return _transporter2.CopyRequest(pairs=pairs)


def build_upload_tables_request(
    request: Request,
    manifest_entries: list[dict],
    option_keys: dict[str, list[str]],
) -> _transporter2.CopyRequest:
    files_by_name: dict[str, str | None] = {
        entry[MANIFEST_NAME_KEY]: entry.get(MANIFEST_FILE_KEY)
        for entry in manifest_entries
    }
    pairs: list[_transporter2.CopyPair] = []
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
        pairs.append(
            _transporter2.CopyPair(
                from_=local_location(Path(source)),
                to=location_for(table.location, option_keys),
            )
        )
    return _transporter2.CopyRequest(pairs=pairs)


def write_transporter_request(request: _transporter2.CopyRequest, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(_transporter2._dump(request), fh, sort_keys=False)


def run_transporter(request_yaml: Path, response_yaml: Path) -> int:
    try:
        binary = PythonEnvironment().binary(TRANSPORTER_BINARY_NAME)
    except BinaryNotFoundError as error:
        raise TransporterNotFoundError() from error
    response_yaml.parent.mkdir(parents=True, exist_ok=True)
    command = [
        str(binary),
        _transporter2._COPY,
        "--request",
        str(request_yaml),
        "--response",
        str(response_yaml),
    ]
    return run_command(command)


def stream_reader(stream: IO[str], level: int) -> threading.Thread:
    def loop() -> None:
        try:
            for line in stream:
                logger.log(level, line.rstrip(), extra={"noformat": True})
        finally:
            stream.close()

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    return thread


def run_command(command: Sequence[str]) -> int:
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,
        text=True,
    )
    assert process.stdout is not None
    assert process.stderr is not None
    stdout_reader = stream_reader(process.stdout, logging.INFO)
    stderr_reader = stream_reader(process.stderr, logging.WARNING)
    return_code = process.wait()
    stdout_reader.join()
    stderr_reader.join()
    return return_code
