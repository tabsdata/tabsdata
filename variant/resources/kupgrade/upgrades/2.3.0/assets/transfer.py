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

TRUE = "true"
FALSE = "false"

AWS_ACCESS_KEY_ID_OPTION = "aws_access_key_id"
AWS_SECRET_ACCESS_KEY_OPTION = "aws_secret_access_key"
AWS_SESSION_TOKEN_OPTION = "aws_session_token"
AWS_REGION_OPTION = "aws_region"
AWS_ENDPOINT_OPTION = "aws_endpoint"
AWS_VIRTUAL_HOSTED_STYLE_REQUEST_OPTION = "aws_virtual_hosted_style_request"
AWS_ALLOW_HTTP_OPTION = "aws_allow_http"
AWS_SKIP_SIGNATURE_OPTION = "aws_skip_signature"

AZURE_STORAGE_ACCOUNT_NAME_OPTION = "azure_storage_account_name"
AZURE_STORAGE_ACCOUNT_KEY_OPTION = "azure_storage_account_key"
AZURE_STORAGE_SAS_TOKEN_OPTION = "azure_storage_sas_token"
AZURE_STORAGE_ENDPOINT_OPTION = "azure_storage_endpoint"
AZURE_ALLOW_HTTP_OPTION = "azure_allow_http"

GOOGLE_SERVICE_ACCOUNT_KEY_OPTION = "google_service_account_key"
GOOGLE_SERVICE_ACCOUNT_OPTION = "google_service_account"
GOOGLE_ALLOW_HTTP_OPTION = "google_allow_http"

S3_SIGNATURE_REQUIREMENT = "access_key_id and secret_access_key, or anonymous"
S3_ADDRESS_REQUIREMENT = "region or endpoint"
AZURE_ACCOUNT_REQUIREMENT = "account_name"
AZURE_SECRET_REQUIREMENT = "account_key or sas_token"
GCS_ACCOUNT_REQUIREMENT = "service_account_key or service_account"


class TransferError(ValueError):
    pass


class StoreKind(StrEnum):
    S3 = "s3"
    AZURE = "azure"
    GCS = "gcs"
    LOCAL = "file"

    @classmethod
    def of(cls, location: str) -> StoreKind:
        scheme = urlparse(location).scheme
        if scheme not in STORE_KIND_BY_SCHEME:
            raise TransferError(f"Location {location} uses an unsupported scheme")
        return STORE_KIND_BY_SCHEME[scheme]

    def canonical(self, key: str) -> str:
        if key in FRIENDLY_KEYS[self]:
            return FRIENDLY_KEYS[self][key]
        prefix = CANONICAL_PREFIXES.get(self)
        if prefix and key.startswith(prefix):
            return key
        raise TransferError(f"Option {key} is not a known {self} option")

    def missing(self, resolved: dict[str, str]) -> str | None:
        if self == StoreKind.S3:
            anonymous = resolved.get(AWS_SKIP_SIGNATURE_OPTION) == TRUE
            signed = (
                AWS_ACCESS_KEY_ID_OPTION in resolved
                and AWS_SECRET_ACCESS_KEY_OPTION in resolved
            )
            if not (anonymous or signed):
                return S3_SIGNATURE_REQUIREMENT
            if (
                AWS_REGION_OPTION not in resolved
                and AWS_ENDPOINT_OPTION not in resolved
            ):
                return S3_ADDRESS_REQUIREMENT
        elif self == StoreKind.AZURE:
            if AZURE_STORAGE_ACCOUNT_NAME_OPTION not in resolved:
                return AZURE_ACCOUNT_REQUIREMENT
            if (
                AZURE_STORAGE_ACCOUNT_KEY_OPTION not in resolved
                and AZURE_STORAGE_SAS_TOKEN_OPTION not in resolved
            ):
                return AZURE_SECRET_REQUIREMENT
        elif self == StoreKind.GCS:
            if (
                GOOGLE_SERVICE_ACCOUNT_KEY_OPTION not in resolved
                and GOOGLE_SERVICE_ACCOUNT_OPTION not in resolved
            ):
                return GCS_ACCOUNT_REQUIREMENT
        return None


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

FRIENDLY_KEYS = {
    StoreKind.S3: {
        "access_key_id": AWS_ACCESS_KEY_ID_OPTION,
        "secret_access_key": AWS_SECRET_ACCESS_KEY_OPTION,
        "session_token": AWS_SESSION_TOKEN_OPTION,
        "region": AWS_REGION_OPTION,
        "endpoint": AWS_ENDPOINT_OPTION,
        "virtual_hosted_style_request": AWS_VIRTUAL_HOSTED_STYLE_REQUEST_OPTION,
        "allow_http": AWS_ALLOW_HTTP_OPTION,
        "anonymous": AWS_SKIP_SIGNATURE_OPTION,
        "skip_signature": AWS_SKIP_SIGNATURE_OPTION,
    },
    StoreKind.AZURE: {
        "account_name": AZURE_STORAGE_ACCOUNT_NAME_OPTION,
        "account_key": AZURE_STORAGE_ACCOUNT_KEY_OPTION,
        "sas_token": AZURE_STORAGE_SAS_TOKEN_OPTION,
        "endpoint": AZURE_STORAGE_ENDPOINT_OPTION,
        "allow_http": AZURE_ALLOW_HTTP_OPTION,
    },
    StoreKind.GCS: {
        "service_account_key": GOOGLE_SERVICE_ACCOUNT_KEY_OPTION,
        "service_account": GOOGLE_SERVICE_ACCOUNT_OPTION,
        "allow_http": GOOGLE_ALLOW_HTTP_OPTION,
    },
    StoreKind.LOCAL: {},
}

CANONICAL_PREFIXES = {
    StoreKind.S3: "aws_",
    StoreKind.AZURE: "azure_",
    StoreKind.GCS: "google_",
}

CLOUD_STORES = {
    StoreKind.S3: S3Store,
    StoreKind.AZURE: AzureStore,
    StoreKind.GCS: GCSStore,
}


def split(uri: str) -> tuple[str, str]:
    root, _, name = uri.rpartition(PATH_SEPARATOR)
    if not root or not name:
        raise TransferError(f"Location {uri} does not name an object")
    return root, name


def local_path(uri: str) -> Path:
    return Path(url2pathname(urlparse(uri).path))


class ObjectStores:
    stores: dict[tuple[str, tuple[tuple[str, str], ...]], ObjectStore]
    lock: threading.Lock

    def __init__(self) -> None:
        self.stores = {}
        self.lock = threading.Lock()

    @staticmethod
    def build(root: str, options: dict[str, str]) -> ObjectStore:
        kind = StoreKind.of(root)
        if kind == StoreKind.LOCAL:
            return LocalStore(local_path(root), mkdir=True, automatic_cleanup=True)
        parsed = urlparse(root)
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

    def store(self, root: str, options: dict[str, str]) -> ObjectStore:
        key = (root, tuple(sorted(options.items())))
        with self.lock:
            if key not in self.stores:
                self.stores[key] = self.build(root, options)
            return self.stores[key]


OBJECT_STORES = ObjectStores()


@dataclass(frozen=True)
class StorageLocation:
    uri: str
    options: dict[str, str] = field(default_factory=dict)

    def stored_object(self) -> tuple[ObjectStore, str]:
        root, name = split(self.uri)
        return OBJECT_STORES.store(root, self.options), name

    def chunks(self) -> Iterator[memoryview]:
        object_store, name = self.stored_object()
        result = obstore.get(object_store, name)
        size = result.meta[SIZE_KEY]
        if size >= STREAM_THRESHOLD:
            yield from self.ranges(object_store, name, size)
            return
        for chunk in result.stream(min_chunk_size=STREAM_CHUNK_SIZE):
            yield memoryview(chunk)

    @staticmethod
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

    def download(self, target: Path) -> int:
        target.parent.mkdir(parents=True, exist_ok=True)
        partial = target.with_name(f"{target.name}{PARTIAL_SUFFIX}")
        written = 0
        try:
            with open(partial, "wb") as handle:
                for chunk in self.chunks():
                    written += handle.write(chunk)
            os.replace(partial, target)
        except BaseException:
            partial.unlink(missing_ok=True)
            raise
        return written

    def size(self) -> int:
        object_store, name = self.stored_object()
        return obstore.head(object_store, name)[SIZE_KEY]

    def read_range(self, start: int, end: int) -> bytes:
        object_store, name = self.stored_object()
        return bytes(obstore.get_range(object_store, name, start=start, end=end))

    def upload(self, source: Path) -> int:
        object_store, name = self.stored_object()
        obstore.put(object_store, name, source)
        return source.stat().st_size
