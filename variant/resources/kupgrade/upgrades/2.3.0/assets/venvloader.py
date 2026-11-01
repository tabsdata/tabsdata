#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse
import contextlib
import io
import os
import shutil
import subprocess
import sys
import tarfile
import time
from pathlib import Path
from typing import IO, Iterator

import obstore.exceptions
import zstandard

from tabsdatak.common._storage.mounts import MountError, NamedMountDeclaration
from tabsdatak.common._storage.transfer import StorageLocation, TransferError

IMAGES_MOUNT_ID = "IMAGES"
IMAGES_SUBFOLDER = "python"
IMAGE_NAME = "tdvenvs.tar.zst"
IMAGE_SUFFIX = ".tar.zst"


UV_FOLDER = ".uv"
KENVS_FOLDER = "kenvs"

CLEAR_ATTEMPTS = 8
CLEAR_DELAY = 1

ENV_UV_MODE = "TD_KUBE_UV_MODE"
UV_MODE_LINKED = "linked"
UV_MODE_LOCAL = "local"
UV_MODES_HOST = (UV_MODE_LINKED, UV_MODE_LOCAL)

TAR_EXECUTABLE = "tar"
COPY_BLOCK_SIZE = 1 << 22

PATH_SEPARATOR = "/"

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
            " uv home. Loading nothing when none is given, so a caller that"
            " already has its environments can keep the step and make it a"
            " no-op"
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


def mount_location(storage_config: Path, mount_id: str) -> StorageLocation:
    declaration = NamedMountDeclaration.read(storage_config, mount_id)
    uri = declaration.resolved_uri()
    return StorageLocation(uri=uri, options=declaration.storage_options(uri))


def image_location(
    mount: StorageLocation, subfolder: str, name: str
) -> StorageLocation:
    segments = [mount.uri.rstrip(PATH_SEPARATOR)]
    if subfolder.strip(PATH_SEPARATOR):
        segments.append(subfolder.strip(PATH_SEPARATOR))
    segments.append(name)
    return StorageLocation(uri=PATH_SEPARATOR.join(segments), options=mount.options)


class ChunkReader(io.RawIOBase):
    def __init__(self, chunks: Iterator[memoryview]) -> None:
        self._chunks = chunks
        self._buffer = memoryview(b"")

    def readable(self) -> bool:
        return True

    def readinto(self, target) -> int:
        while not self._buffer:
            try:
                self._buffer = next(self._chunks)
            except StopIteration:
                return 0
        size = min(len(target), len(self._buffer))
        target[:size] = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return size

    def close(self) -> None:
        self._chunks.close()
        super().close()


@contextlib.contextmanager
def open_image(location: StorageLocation) -> Iterator[IO[bytes]]:
    trace(f"Downloading {location.uri}")
    stream = io.BufferedReader(ChunkReader(location.chunks()))
    try:
        yield stream
    finally:
        stream.close()


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


def extract_image(location: StorageLocation, uv_home: Path) -> None:
    use_tar = shutil.which(TAR_EXECUTABLE) is not None
    with open_image(location) as body:
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
    mode = os.environ.get(ENV_UV_MODE, "").strip().lower()
    if mode in UV_MODES_HOST:
        trace(f"Skipping the load; {ENV_UV_MODE} is {mode}")
        return 0
    venvs = arguments.venvs
    if not venvs:
        trace("Skipping the load; no --venv was requested")
        return 0
    output = Path(arguments.output)
    uv_home = output / UV_FOLDER

    output.mkdir(parents=True, exist_ok=True)
    try:
        mount = mount_location(Path(arguments.storage_config), arguments.mount)
        for venv in venvs:
            clear_venv(uv_home, venv)
            name = image_for_venv(arguments.image, venv)
            extract_image(image_location(mount, arguments.subfolder, name), uv_home)
    except (
        OSError,
        MountError,
        TransferError,
        obstore.exceptions.BaseError,
    ) as error:
        die("Loading the image", str(error))

    trace(f"Done! Virtual environments available at {uv_home}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
