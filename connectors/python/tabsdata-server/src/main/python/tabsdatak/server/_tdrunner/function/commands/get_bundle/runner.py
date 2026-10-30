#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

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
from tabsdatak.server._tdrunner.function.commands.get_bundle.options import (
    GetBundleArguments,
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


def extract(bundle_archive: Path, bundle_folder: Path) -> None:
    try:
        extract_bundle(bundle_archive, bundle_folder)
    except (tarfile.TarError, FileNotFoundError, zstandard.ZstdError) as error:
        raise GetBundleErrorCode.GB_4.exception(cause=error, detail=str(error))


def run(arguments: GetBundleArguments) -> int:
    logger.info("Getting the function bundle")
    request = AnyRequest.load(arguments.request, GetBundleErrorCode.GB_1)
    bundle = request.info.function_bundle
    logger.debug(
        f"Bundle {bundle.uri} goes to archive {arguments.bundle_archive} "
        f"and folder {arguments.bundle_folder}"
    )
    fetch(storage_location(bundle), arguments.bundle_archive)
    logger.info(f"Extracting the bundle archive into {arguments.bundle_folder}")
    extract(arguments.bundle_archive, arguments.bundle_folder)
    logger.info("Function bundle ready")
    return 0
