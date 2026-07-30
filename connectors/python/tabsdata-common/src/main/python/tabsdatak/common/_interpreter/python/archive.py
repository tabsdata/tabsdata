#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import yaml

from tabsdatak.common._transporter.handler.handler2 import (
    TransporterHandler2,
    local_location,
)
from tabsdatak.common._transporter.invoker import transporter2

logger = logging.getLogger(__name__)

STORAGE_KEY = "storage"
MOUNTS_KEY = "mounts"
MOUNT_ID_KEY = "id"
MOUNT_URI_KEY = "uri"
MOUNT_OPTIONS_KEY = "options"

SECRET_ENV_PREFIX = "SECRETS__STORAGE__MOUNTS__"
SECRET_ENV_OPTIONS_INFIX = "__OPTIONS__"

IMAGES_MOUNT_ID = "IMAGES"
IMAGES_SUBFOLDER = "python"


class MountNotFoundError(ValueError):
    pass


def secret_env_name(mount_id: str, option_key: str) -> str:
    return (
        f"{SECRET_ENV_PREFIX}{mount_id}{SECRET_ENV_OPTIONS_INFIX}{option_key.upper()}"
    )


def load_mount(storage_config: Path, mount_id: str) -> tuple[str, list[str]]:
    document = yaml.safe_load(storage_config.read_text(encoding="utf-8"))
    mounts = (document or {}).get(STORAGE_KEY, {}).get(MOUNTS_KEY, []) or []
    for mount in mounts:
        if mount.get(MOUNT_ID_KEY) != mount_id:
            continue
        uri = mount.get(MOUNT_URI_KEY)
        if not uri:
            raise MountNotFoundError(
                f"Mount {mount_id!r} in {storage_config} declares no uri."
            )
        options = mount.get(MOUNT_OPTIONS_KEY) or {}
        return uri, list(options.keys())
    raise MountNotFoundError(f"Mount {mount_id!r} not found in {storage_config}.")


def target_location(
    uri: str, mount_id: str, option_keys: list[str], subfolder: str, name: str
) -> transporter2.Location:
    prefix = uri if uri.endswith("/") else f"{uri}/"
    if subfolder:
        prefix = f"{prefix}{subfolder.strip('/')}/"
    configs = {key: f"env:{secret_env_name(mount_id, key)}" for key in option_keys}
    return transporter2.Location(uri=f"{prefix}{name}", configs=configs)


def send_image(
    image: Path,
    storage_config: Path,
    transporter_request: Path,
    transporter_response: Path,
    mount_id: str = IMAGES_MOUNT_ID,
    subfolder: str = IMAGES_SUBFOLDER,
) -> None:
    if not image.is_file():
        raise FileNotFoundError(f"Image not found: {image}")
    uri, option_keys = load_mount(storage_config, mount_id)
    handler = TransporterHandler2()
    source = local_location(image)
    target = target_location(uri, mount_id, option_keys, subfolder, image.name)
    request = handler.make_request([(source, target)])
    handler.write_request(request, transporter_request)
    logger.info("Sending %s to %s", image, target.uri)
    handler.run_or_raise(transporter_request, transporter_response)


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="archive",
        description="Send a built image to a storage mount using transporter2.",
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
        "--transporter-request",
        required=True,
        help="Transporter request file to write",
    )
    parser.add_argument(
        "--transporter-response",
        required=True,
        help="Transporter response file to write",
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
    send_image(
        Path(arguments.image),
        Path(arguments.storage_config),
        Path(arguments.transporter_request),
        Path(arguments.transporter_response),
        mount_id=arguments.mount,
        subfolder=arguments.subfolder,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
