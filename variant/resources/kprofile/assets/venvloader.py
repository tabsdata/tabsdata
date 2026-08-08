#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse
import contextlib
import os
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.parse
from pathlib import Path
from typing import IO, Iterator

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
SECRET_ENV_URI_SUFFIX = "__URI"

PLACEHOLDER_PREFIX = "${{"

IMAGES_MOUNT_ID = "IMAGES"
IMAGES_SUBFOLDER = "python"
IMAGE_NAME = "tdvenvs.tar.zst"
IMAGE_SUFFIX = ".tar.zst"

VENV_TD = "td"

UV_FOLDER = ".uv"
KENVS_FOLDER = "kenvs"

CLEAR_ATTEMPTS = 8
CLEAR_DELAY = 1

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

TAR_EXECUTABLE = "tar"
COPY_BLOCK_SIZE = 1 << 22

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
        help=(
            "Image file name; the venv name is appended to the stem"
            f" (default: {IMAGE_NAME})"
        ),
    )
    parser.add_argument(
        "--venv",
        action="append",
        dest="venvs",
        metavar="VENV",
        help=(
            "Virtual environment to load; repeat to load several into the same"
            f" uv home (default: {VENV_TD})"
        ),
    )
    return parser.parse_args()


def image_for_venv(image: str, venv: str) -> str:
    stem = image[: -len(IMAGE_SUFFIX)] if image.endswith(IMAGE_SUFFIX) else image
    return f"{stem}-{venv}{IMAGE_SUFFIX}"


def human_size(size: int) -> str:
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{int(value)} {unit}" if unit == "B" else f"{value:.2f} {unit}"
        value /= 1024
    raise AssertionError


def secret_env_name(mount_id: str, option_key: str) -> str:
    return (
        f"{SECRET_ENV_PREFIX}{mount_id}{SECRET_ENV_OPTIONS_INFIX}{option_key.upper()}"
    )


def secret_uri_env_name(mount_id: str) -> str:
    return f"{SECRET_ENV_PREFIX}{mount_id}{SECRET_ENV_URI_SUFFIX}"


def resolve_uri(uri: str, mount_id: str) -> str:
    if not uri.startswith(PLACEHOLDER_PREFIX):
        return uri
    variable = secret_uri_env_name(mount_id)
    resolved = os.environ.get(variable, "").strip()
    if not resolved:
        die(
            "Reading the mount",
            f"Mount {mount_id!r} holds the placeholder {uri!r} and the environment "
            f"variable {variable} is unset or empty",
        )
    return resolved


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
        return resolve_uri(str(uri), mount_id), resolved
    die("Reading the mount", f"Mount {mount_id!r} not found in {storage_config}")
    raise AssertionError


def image_uri(uri: str, subfolder: str, name: str) -> str:
    prefix = uri if uri.endswith("/") else f"{uri}/"
    if subfolder:
        prefix = f"{prefix}{subfolder.strip('/')}/"
    return f"{prefix}{name}"


@contextlib.contextmanager
def open_file(source: str) -> Iterator[IO[bytes]]:
    parsed = urllib.parse.urlparse(source)
    origin = Path(urllib.parse.unquote(parsed.path))
    if not origin.is_file():
        die("Downloading the image", f"Image not found: {origin}")
    trace(f"Reading {origin}")
    with open(origin, "rb") as reader:
        yield reader


@contextlib.contextmanager
def open_s3(source: str, options: dict[str, str]) -> Iterator[IO[bytes]]:
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
        body = client.get_object(Bucket=bucket, Key=key)["Body"]
    except botocore.exceptions.ClientError as error:
        die("Downloading the image", f"{source}: {error}")
    except botocore.exceptions.BotoCoreError as error:
        die("Downloading the image", f"{source}: {error}")
    try:
        yield body
    finally:
        body.close()


@contextlib.contextmanager
def open_image(source: str, options: dict[str, str]) -> Iterator[IO[bytes]]:
    scheme = urllib.parse.urlparse(source).scheme or SCHEME_FILE
    if scheme == SCHEME_FILE:
        with open_file(source) as stream:
            yield stream
    elif scheme == SCHEME_S3:
        with open_s3(source, options) as stream:
            yield stream
    else:
        die("Downloading the image", f"Unsupported storage scheme: {scheme}")


def clear_venv(uv_home: Path, venv: str) -> None:
    target = uv_home / KENVS_FOLDER / venv
    if target.exists():
        trace(f"Clearing {target}")
        for attempt in range(CLEAR_ATTEMPTS):
            try:
                shutil.rmtree(target)
                break
            except OSError as error:
                if attempt == CLEAR_ATTEMPTS - 1:
                    raise
                trace(f"Clearing {target} failed ({error}); retrying...")
                time.sleep(CLEAR_DELAY)
    uv_home.mkdir(parents=True, exist_ok=True)


def extract_with_tar(stream: IO[bytes], uv_home: Path) -> int:
    process = subprocess.Popen(
        [TAR_EXECUTABLE, "-xf", "-", "-C", str(uv_home)],
        stdin=subprocess.PIPE,
    )
    copied = 0
    try:
        while True:
            block = stream.read(COPY_BLOCK_SIZE)
            if not block:
                break
            process.stdin.write(block)
            copied += len(block)
    finally:
        process.stdin.close()
        status = process.wait()
    if status != 0:
        die("Extracting the image", f"{TAR_EXECUTABLE} exited with status {status}")
    return copied


def extract_with_tarfile(stream: IO[bytes], uv_home: Path) -> None:
    with tarfile.open(fileobj=stream, mode="r|") as archive:
        archive.extractall(path=uv_home, filter=None)


def extract_image(source: str, options: dict[str, str], uv_home: Path) -> None:
    use_tar = shutil.which(TAR_EXECUTABLE) is not None
    with open_image(source, options) as body:
        decompressor = zstandard.ZstdDecompressor()
        with decompressor.stream_reader(body) as stream:
            trace(
                f"Extracting into {uv_home}"
                f" ({'system tar' if use_tar else 'python tarfile'})"
            )
            if use_tar:
                size = extract_with_tar(stream, uv_home)
                trace(f"Unpacked {human_size(size)} ({size} bytes) of archive content")
            else:
                extract_with_tarfile(stream, uv_home)


def main() -> int:
    arguments = parse_arguments()
    if os.environ.get(ENV_UV_MODE, "").strip().lower() == UV_MODE_LOCAL:
        trace(f"Skipping the load; {ENV_UV_MODE} is {UV_MODE_LOCAL}")
        return 0
    output = Path(arguments.output)
    uv_home = output / UV_FOLDER

    uri, options = load_mount(Path(arguments.storage_config), arguments.mount)
    venvs = arguments.venvs or [VENV_TD]

    output.mkdir(parents=True, exist_ok=True)
    for venv in venvs:
        clear_venv(uv_home, venv)
        name = image_for_venv(arguments.image, venv)
        extract_image(image_uri(uri, arguments.subfolder, name), options, uv_home)

    trace(f"Done! Virtual environments available at {uv_home}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
