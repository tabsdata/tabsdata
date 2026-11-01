#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from pathlib import Path

from tabsdatak.common._storage.mounts import MountError
from tabsdatak.common._storage.transfer import StorageLocation
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    AnyRequest,
)
from tabsdatak.server._tdrunner.function._commons.run.command import Command
from tabsdatak.server._tdrunner.function._handlers.bundle import (
    BundleArchive,
    DownloadError,
    ExtractError,
)
from tabsdatak.server._tdrunner.function.commands.gathering.fetch_bundle.error import (
    FetchBundleErrorCode,
)
from tabsdatak.server._tdrunner.function.commands.gathering.fetch_bundle.options import (  # noqa: E501
    FetchBundleArguments,
)

logger = logging.getLogger(__name__)


class FetchBundle(Command[FetchBundleArguments]):
    request_file: Path
    bundle_folder: Path
    archive: BundleArchive
    request: AnyRequest

    def __init__(self, arguments: FetchBundleArguments) -> None:
        super().__init__(arguments)
        self.request_file = arguments.request
        self.bundle_folder = arguments.bundle_folder
        self.archive = BundleArchive(arguments.bundle_archive)

    def run(self) -> int:
        logger.info("Getting the function bundle")
        self.request = AnyRequest.load(self.request_file, FetchBundleErrorCode.FB_1)
        self.download()
        self.extract()
        logger.info("Function bundle ready")
        return 0

    def download(self) -> None:
        source = self.source()
        logger.debug(
            f"Bundle {source.uri} goes to archive {self.archive.archive} "
            f"and folder {self.bundle_folder}"
        )
        try:
            self.archive.download(source)
        except DownloadError as error:
            raise FetchBundleErrorCode.FB_3.exception(cause=error, detail=error.detail)

    def source(self) -> StorageLocation:
        try:
            return self.request.info.function_bundle.storage_location()
        except MountError as error:
            raise FetchBundleErrorCode.FB_2.exception(cause=error, detail=str(error))

    def extract(self) -> None:
        logger.info(f"Extracting the bundle archive into {self.bundle_folder}")
        try:
            self.archive.extract(self.bundle_folder)
        except ExtractError as error:
            raise FetchBundleErrorCode.FB_4.exception(cause=error, detail=error.detail)
