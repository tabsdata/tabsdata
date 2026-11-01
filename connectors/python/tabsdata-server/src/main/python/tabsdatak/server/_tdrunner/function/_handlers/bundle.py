#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import tarfile
from pathlib import Path

import obstore.exceptions
import zstandard

from tabsdatak.common._storage.transfer import StorageLocation, TransferError

logger = logging.getLogger(__name__)


class BundleError(Exception):
    detail: str

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class DownloadError(BundleError):
    pass


class ExtractError(BundleError):
    pass


class BundleArchive:
    archive: Path

    def __init__(self, archive: Path) -> None:
        self.archive = archive

    def download(self, source: StorageLocation) -> None:
        try:
            size = source.download(self.archive)
        except (OSError, TransferError, obstore.exceptions.BaseError) as error:
            raise DownloadError(str(error)) from error
        logger.debug(f"Bundle archive of {size} bytes downloaded to {self.archive}")

    # Streamed, so memory stays bounded on large bundles; the `data` filter
    # refuses members that would escape the folder (absolute paths, `..`, links).
    def extract(self, folder: Path) -> None:
        folder.mkdir(parents=True, exist_ok=True)
        try:
            with open(self.archive, "rb") as raw:
                with zstandard.ZstdDecompressor().stream_reader(raw) as stream:
                    with tarfile.open(fileobj=stream, mode="r|") as tar:
                        tar.extractall(folder, filter="data")
        except (tarfile.TarError, FileNotFoundError, zstandard.ZstdError) as error:
            raise ExtractError(str(error)) from error
