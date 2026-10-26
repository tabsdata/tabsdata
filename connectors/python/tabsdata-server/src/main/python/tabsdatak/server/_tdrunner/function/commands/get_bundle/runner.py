#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse
import logging
import tarfile
from pathlib import Path

import obstore.exceptions
import zstandard

from tabsdatak.common._storage import transfer
from tabsdatak.common._storage.mounts import MountError
from tabsdatak.common._storage.transfer import StorageLocation, TransferError
from tabsdatak.server._tdrunner.function._commons.document.request import (
    AnyRequest,
    Location,
)
from tabsdatak.server._tdrunner.function._lib.bundle import extract_bundle
from tabsdatak.server._tdrunner.function.commands.get_bundle.error import (
    GetBundleErrorCode,
)

logger = logging.getLogger(__name__)


def storage_location(location: Location) -> StorageLocation:
    try:
        declaration = location.mount_declaration()
        return StorageLocation(
            uri=location.uri,
            options=declaration.storage_options(location.uri),
        )
    except MountError as error:
        raise GetBundleErrorCode.GB_2.exception(cause=error, detail=str(error))


def fetch(source: StorageLocation, bundle_archive: Path) -> None:
    try:
        size = transfer.download(source, bundle_archive)
    except (OSError, TransferError, obstore.exceptions.BaseError) as error:
        raise GetBundleErrorCode.GB_3.exception(cause=error, detail=str(error))
    logger.debug(f"Bundle archive of {size} bytes downloaded to {bundle_archive}")


def extract(bundle_archive: Path, bundle_dir: Path) -> None:
    try:
        extract_bundle(bundle_archive, bundle_dir)
    except (tarfile.TarError, FileNotFoundError, zstandard.ZstdError) as error:
        raise GetBundleErrorCode.GB_4.exception(cause=error, detail=str(error))


def run(arguments: argparse.Namespace) -> int:
    request_path = Path(arguments.request)
    bundle_dir = Path(arguments.bundle_dir)
    bundle_archive = Path(arguments.bundle_archive)

    logger.info("Getting the function bundle")
    request = AnyRequest.load(request_path, GetBundleErrorCode.GB_1)
    bundle = request.info.function_bundle
    logger.debug(
        f"Bundle {bundle.uri} goes to archive {bundle_archive} and folder {bundle_dir}"
    )
    fetch(storage_location(bundle), bundle_archive)
    logger.info(f"Extracting the bundle archive into {bundle_dir}")
    extract(bundle_archive, bundle_dir)
    logger.info("Function bundle ready")
    return 0
