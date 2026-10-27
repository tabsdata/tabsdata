#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import threading
from collections import deque
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import StrEnum
from itertools import islice
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

import obstore
from obstore.store import AzureStore, GCSStore, LocalStore, ObjectStore, S3Store

MEBIBYTE = 1024 * 1024
STREAM_THRESHOLD = 256 * MEBIBYTE
STREAM_CHUNK_SIZE = 8 * MEBIBYTE
RANGE_SIZE = 16 * MEBIBYTE
RANGE_CONCURRENCY = 8

ACCOUNT_SEPARATOR = "@"
PATH_SEPARATOR = "/"
PARTIAL_SUFFIX = ".partial"
SIZE_KEY = "size"

AMBIENT_PREFIXES = ("AWS_", "AZURE_", "GOOGLE_")


class StoreKind(StrEnum):
    S3 = "s3"
    AZURE = "azure"
    GCS = "gcs"
    LOCAL = "file"


STORE_KIND_BY_SCHEME = {
    "s3": StoreKind.S3,
    "az": StoreKind.AZURE,
    "azure": StoreKind.AZURE,
    "abfs": StoreKind.AZURE,
    "abfss": StoreKind.AZURE,
    "adl": StoreKind.AZURE,
    "gs": StoreKind.GCS,
    "gcs": StoreKind.GCS,
    "file": StoreKind.LOCAL,
}

CLOUD_STORES = {
    StoreKind.S3: S3Store,
    StoreKind.AZURE: AzureStore,
    StoreKind.GCS: GCSStore,
}

STORES: dict[tuple[str, tuple[tuple[str, str], ...]], ObjectStore] = {}
STORES_LOCK = threading.Lock()


class TransferError(ValueError):
    pass


@dataclass(frozen=True)
class StorageLocation:
    uri: str
    options: dict[str, str] = field(default_factory=dict)


def split(uri: str) -> tuple[str, str]:
    root, _, name = uri.rpartition(PATH_SEPARATOR)
    if not root or not name:
        raise TransferError(f"Location {uri} does not name an object")
    return root, name


def local_path(uri: str) -> Path:
    return Path(url2pathname(urlparse(uri).path))


def build(root: str, options: dict[str, str]) -> ObjectStore:
    parsed = urlparse(root)
    kind = STORE_KIND_BY_SCHEME.get(parsed.scheme)
    if kind is None:
        raise TransferError(f"Location {root} uses an unsupported scheme")
    if kind == StoreKind.LOCAL:
        return LocalStore(local_path(root), mkdir=True)
    container = parsed.netloc.partition(ACCOUNT_SEPARATOR)[0]
    prefix = parsed.path.strip(PATH_SEPARATOR) or None
    ambient = {
        name: os.environ.pop(name)
        for name in list(os.environ)
        if name.startswith(AMBIENT_PREFIXES)
    }
    try:
        return CLOUD_STORES[kind](container, prefix=prefix, config=options)
    finally:
        os.environ.update(ambient)


def store(root: str, options: dict[str, str]) -> ObjectStore:
    key = (root, tuple(sorted(options.items())))
    with STORES_LOCK:
        if key not in STORES:
            STORES[key] = build(root, options)
        return STORES[key]


def ranges(object_store: ObjectStore, name: str, size: int) -> Iterator[memoryview]:
    starts = iter(range(0, size, RANGE_SIZE))
    with ThreadPoolExecutor(max_workers=RANGE_CONCURRENCY) as executor:
        pending: deque[Future] = deque()

        def request(start: int) -> None:
            pending.append(
                executor.submit(
                    obstore.get_range,
                    object_store,
                    name,
                    start=start,
                    end=min(start + RANGE_SIZE, size),
                ),
            )

        for start in islice(starts, RANGE_CONCURRENCY):
            request(start)
        while pending:
            data = pending.popleft().result()
            following = next(starts, None)
            if following is not None:
                request(following)
            yield memoryview(data)


def chunks(source: StorageLocation) -> Iterator[memoryview]:
    root, name = split(source.uri)
    object_store = store(root, source.options)
    result = obstore.get(object_store, name)
    size = result.meta[SIZE_KEY]
    if size >= STREAM_THRESHOLD:
        yield from ranges(object_store, name, size)
        return
    for chunk in result.stream(min_chunk_size=STREAM_CHUNK_SIZE):
        yield memoryview(chunk)


def download(source: StorageLocation, target: Path) -> int:
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_name(f"{target.name}{PARTIAL_SUFFIX}")
    written = 0
    try:
        with open(partial, "wb") as handle:
            for chunk in chunks(source):
                written += handle.write(chunk)
        os.replace(partial, target)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
    return written


def size(source: StorageLocation) -> int:
    root, name = split(source.uri)
    return obstore.head(store(root, source.options), name)[SIZE_KEY]


def read_range(source: StorageLocation, start: int, end: int) -> bytes:
    root, name = split(source.uri)
    return bytes(
        obstore.get_range(store(root, source.options), name, start=start, end=end)
    )


def upload(source: Path, target: StorageLocation) -> int:
    root, name = split(target.uri)
    obstore.put(store(root, target.options), name, source)
    return source.stat().st_size
