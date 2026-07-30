#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse
import os
import shutil
import sys
import tarfile
import time
import urllib.parse
from pathlib import Path

import boto3
import botocore.exceptions
import yaml
import zstandard

STORAGE_KEY = "storage"
MOUNTS_KEY = "mounts"
MOUNT_ID_KEY = "id"
MOUNT_URI_KEY = "uri"
MOUNT_OPTIONS_KEY = "options"

SECRET_ENV_PREFIX = "SECRETS__STORAGE__MOUNTS__"
SECRET_ENV_OPTIONS_INFIX = "__OPTIONS__"

IMAGES_MOUNT_ID = "IMAGES"
IMAGES_SUBFOLDER = "python"
IMAGE_NAME = "tdvenvs.tar.zst"

UV_FOLDER = ".uv"

SCHEME_FILE = "file"
SCHEME_S3 = "s3"

OPTION_REGION = "aws_region"
OPTION_ACCESS_KEY_ID = "aws_access_key_id"
OPTION_SECRET_ACCESS_KEY = "aws_secret_access_key"

ENV_REGION = "AWS_REGION"
ENV_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"
ENV_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"

ENV_UV_MODE = "TD_KUBE_UV_MODE"
UV_MODE_LOCAL = "local"

STARTED = time.monotonic()


def trace(message: str) -> None:
    print(f">> [{time.monotonic() - STARTED:8.2f}s] {message}", flush=True)


def die(description: str, cause: str) -> None:
    raise SystemExit(f"{description} failed: {cause}")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="venvloader",
        description=(
            "Downloads the python virtual environments image from a storage mount "
            "and extracts it into the runtimes folder."
        ),
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Runtimes folder holding the uv home",
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
    parser.add_argument(
        "--image",
        default=IMAGE_NAME,
        help=f"Image file name (default: {IMAGE_NAME})",
    )
    return parser.parse_args()


def secret_env_name(mount_id: str, option_key: str) -> str:
    return (
        f"{SECRET_ENV_PREFIX}{mount_id}{SECRET_ENV_OPTIONS_INFIX}{option_key.upper()}"
    )


def load_mount(storage_config: Path, mount_id: str) -> tuple[str, dict[str, str]]:
    try:
        document = yaml.safe_load(storage_config.read_text(encoding="utf-8"))
    except OSError as error:
        die("Reading the storage configuration", str(error))
    mounts = (document or {}).get(STORAGE_KEY, {}).get(MOUNTS_KEY, []) or []
    for mount in mounts:
        if mount.get(MOUNT_ID_KEY) != mount_id:
            continue
        uri = mount.get(MOUNT_URI_KEY)
        if not uri:
            die("Reading the mount", f"Mount {mount_id!r} declares no uri")
        options = mount.get(MOUNT_OPTIONS_KEY) or {}
        resolved = {}
        for key in options:
            value = os.environ.get(secret_env_name(mount_id, key), "")
            if value:
                resolved[key] = value
        return str(uri), resolved
    die("Reading the mount", f"Mount {mount_id!r} not found in {storage_config}")
    raise AssertionError


def image_uri(uri: str, subfolder: str, name: str) -> str:
    prefix = uri if uri.endswith("/") else f"{uri}/"
    if subfolder:
        prefix = f"{prefix}{subfolder.strip('/')}/"
    return f"{prefix}{name}"


def fetch_file(source: str, target: Path) -> None:
    parsed = urllib.parse.urlparse(source)
    origin = Path(urllib.parse.unquote(parsed.path))
    if not origin.is_file():
        die("Downloading the image", f"Image not found: {origin}")
    trace(f"Copying {origin}")
    shutil.copyfile(origin, target)


def fetch_s3(source: str, target: Path, options: dict[str, str]) -> None:
    parsed = urllib.parse.urlparse(source)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        die("Downloading the image", f"Malformed s3 uri: {source}")
    client = boto3.client(
        "s3",
        region_name=options.get(OPTION_REGION) or os.environ.get(ENV_REGION),
        aws_access_key_id=options.get(OPTION_ACCESS_KEY_ID)
        or os.environ.get(ENV_ACCESS_KEY_ID),
        aws_secret_access_key=options.get(OPTION_SECRET_ACCESS_KEY)
        or os.environ.get(ENV_SECRET_ACCESS_KEY),
    )
    trace(f"Downloading {source}")
    try:
        client.download_file(bucket, key, str(target))
    except botocore.exceptions.ClientError as error:
        die("Downloading the image", f"{source}: {error}")
    except botocore.exceptions.BotoCoreError as error:
        die("Downloading the image", f"{source}: {error}")


def fetch_image(source: str, target: Path, options: dict[str, str]) -> None:
    scheme = urllib.parse.urlparse(source).scheme or SCHEME_FILE
    if scheme == SCHEME_FILE:
        fetch_file(source, target)
    elif scheme == SCHEME_S3:
        fetch_s3(source, target, options)
    else:
        die("Downloading the image", f"Unsupported storage scheme: {scheme}")


def clear_uv_home(uv_home: Path) -> None:
    if uv_home.exists():
        trace(f"Clearing {uv_home}")
        shutil.rmtree(uv_home)
    uv_home.mkdir(parents=True, exist_ok=True)


def extract_image(image: Path, uv_home: Path) -> None:
    trace(f"Extracting into {uv_home}")
    with open(image, "rb") as reader:
        decompressor = zstandard.ZstdDecompressor()
        with decompressor.stream_reader(reader) as stream:
            with tarfile.open(fileobj=stream, mode="r|") as archive:
                archive.extractall(path=uv_home, filter="tar")


def main() -> int:
    arguments = parse_arguments()
    if os.environ.get(ENV_UV_MODE, "").strip().lower() == UV_MODE_LOCAL:
        trace(f"Skipping the load; {ENV_UV_MODE} is {UV_MODE_LOCAL}")
        return 0
    output = Path(arguments.output)
    uv_home = output / UV_FOLDER

    uri, options = load_mount(Path(arguments.storage_config), arguments.mount)
    source = image_uri(uri, arguments.subfolder, arguments.image)

    output.mkdir(parents=True, exist_ok=True)
    image = output / arguments.image
    fetch_image(source, image, options)
    clear_uv_home(uv_home)
    extract_image(image, uv_home)
    image.unlink(missing_ok=True)

    trace(f"Done! Virtual environments available at {uv_home}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
