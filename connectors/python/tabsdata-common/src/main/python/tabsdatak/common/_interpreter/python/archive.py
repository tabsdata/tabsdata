#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import obstore.exceptions

from tabsdatak.common._configuration.document.error import DocumentException
from tabsdatak.common._configuration.document.loader import load
from tabsdatak.common._configuration.model.storage import StorageConfiguration
from tabsdatak.common._storage import transfer
from tabsdatak.common._storage.mounts import MountError
from tabsdatak.common._storage.transfer import StorageLocation, TransferError

logger = logging.getLogger(__name__)

IMAGES_MOUNT_ID = "IMAGES"
IMAGES_SUBFOLDER = "python"

PATH_SEPARATOR = "/"


def target_uri(mount_uri: str, subfolder: str, name: str) -> str:
    prefix = mount_uri.rstrip(PATH_SEPARATOR)
    if subfolder.strip(PATH_SEPARATOR):
        prefix = f"{prefix}{PATH_SEPARATOR}{subfolder.strip(PATH_SEPARATOR)}"
    return f"{prefix}{PATH_SEPARATOR}{name}"


def target_location(
    storage_config: Path, mount_id: str, subfolder: str, name: str
) -> StorageLocation:
    configuration = load(storage_config, StorageConfiguration)
    declaration = configuration.mount(mount_id).declaration()
    uri = target_uri(declaration.resolved_uri(), subfolder, name)
    return StorageLocation(uri=uri, options=declaration.storage_options(uri))


def send_image(
    image: Path,
    storage_config: Path,
    mount_id: str = IMAGES_MOUNT_ID,
    subfolder: str = IMAGES_SUBFOLDER,
) -> int:
    if not image.is_file():
        raise FileNotFoundError(f"Image not found: {image}")
    target = target_location(storage_config, mount_id, subfolder, image.name)
    logger.info(f"Sending {image} to {target.uri}")
    return transfer.upload(image, target)


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="archive",
        description="Send a built image to a storage mount.",
    )
    parser.add_argument(
        "--image",
        required=True,
        help="Image file to send",
    )
    parser.add_argument(
        "--storage-config",
        required=True,
        help="Path to apiserver.storage.yaml",
    )
    parser.add_argument(
        "--mount",
        default=IMAGES_MOUNT_ID,
        help=f"Mount id (default: {IMAGES_MOUNT_ID})",
    )
    parser.add_argument(
        "--subfolder",
        default=IMAGES_SUBFOLDER,
        help=f"Subfolder under the mount (default: {IMAGES_SUBFOLDER})",
    )
    arguments = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        size = send_image(
            Path(arguments.image),
            Path(arguments.storage_config),
            mount_id=arguments.mount,
            subfolder=arguments.subfolder,
        )
    except (
        OSError,
        DocumentException,
        MountError,
        TransferError,
        obstore.exceptions.BaseError,
    ) as error:
        logger.error(f"Cannot send the image {arguments.image}: {error}")
        return 1
    logger.info(f"Sent {size} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
