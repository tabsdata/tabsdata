#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from pathlib import Path
from typing import Any

from tabsdatak.common._transporter.facade import TRANSPORTER_MODE
from tabsdatak.server._tdrunner.function._lib import transporter1, transporter2
from tabsdatak.server._tdrunner.function._lib.errors import CommandErrorCode
from tabsdatak.server._tdrunner.function._lib.mounts import load_mount_option_keys
from tabsdatak.server._tdrunner.function._lib.request import (
    AnyRequest,
    Request,
)

match TRANSPORTER_MODE:
    case 1:
        OutputTableNotInManifestError = transporter1.OutputTableNotInManifestError
        localized_input_files = transporter1.localized_input_files
    case 2:
        OutputTableNotInManifestError = transporter2.OutputTableNotInManifestError
        localized_input_files = transporter2.localized_input_files
    case _:
        raise CommandErrorCode.CMD_5.exception(mode=TRANSPORTER_MODE)


def build_localize_state_request(
    request: Request,
    in_dir: Path,
    state_dir: Path,
    storage_config_file: Path,
) -> Any:
    match TRANSPORTER_MODE:
        case 1:
            return transporter1.build_localize_state_request(request, in_dir, state_dir)
        case 2:
            option_keys = load_mount_option_keys(storage_config_file)
            return transporter2.build_localize_state_request(
                request, in_dir, state_dir, option_keys
            )
        case _:
            raise CommandErrorCode.CMD_5.exception(mode=TRANSPORTER_MODE)


def build_bundle_fetch_request(
    request: Request | AnyRequest,
    target_path: Path,
    storage_config_file: Path,
) -> Any:
    match TRANSPORTER_MODE:
        case 1:
            return transporter1.build_bundle_fetch_request(request, target_path)
        case 2:
            option_keys = load_mount_option_keys(storage_config_file)
            return transporter2.build_bundle_fetch_request(
                request, target_path, option_keys
            )
        case _:
            raise CommandErrorCode.CMD_5.exception(mode=TRANSPORTER_MODE)


def build_upload_state_request(
    request: Request,
    state_dir: Path,
    storage_config_file: Path,
) -> Any:
    match TRANSPORTER_MODE:
        case 1:
            return transporter1.build_upload_state_request(request, state_dir)
        case 2:
            option_keys = load_mount_option_keys(storage_config_file)
            return transporter2.build_upload_state_request(
                request, state_dir, option_keys
            )
        case _:
            raise CommandErrorCode.CMD_5.exception(mode=TRANSPORTER_MODE)


def build_upload_tables_request(
    request: Request,
    manifest_entries: list[dict[str, Any]],
    storage_config_file: Path,
) -> Any:
    match TRANSPORTER_MODE:
        case 1:
            return transporter1.build_upload_tables_request(request, manifest_entries)
        case 2:
            option_keys = load_mount_option_keys(storage_config_file)
            return transporter2.build_upload_tables_request(
                request, manifest_entries, option_keys
            )
        case _:
            raise CommandErrorCode.CMD_5.exception(mode=TRANSPORTER_MODE)


def write_transporter_request(request: Any, path: Path) -> None:
    match TRANSPORTER_MODE:
        case 1:
            transporter1.write_transporter_request(request, path)
        case 2:
            transporter2.write_transporter_request(request, path)
        case _:
            raise CommandErrorCode.CMD_5.exception(mode=TRANSPORTER_MODE)


def run_transporter(request_yaml: Path, response_or_report: Path) -> int:
    match TRANSPORTER_MODE:
        case 1:
            return transporter1.run_transporter(request_yaml, response_or_report)
        case 2:
            return transporter2.run_transporter(request_yaml, response_or_report)
        case _:
            raise CommandErrorCode.CMD_5.exception(mode=TRANSPORTER_MODE)
