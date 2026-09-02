#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``gcs`` backend."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import ClassVar

from pydantic.dataclasses import dataclass

from ..backend import Domain, System, require


@dataclass(frozen=True)
class GCSSystem(System):
    system_type: ClassVar[str] = "gcs"
    name: str
    system_kind: str
    sys_path: PurePosixPath
    bucket: str
    service_account_key: str
    # S3-interop HMAC keys (optional; present when the system carries S3-compat creds)
    hmac_access_key: str | None = None
    hmac_secret_key: str | None = None


@dataclass(frozen=True)
class GCSDomain(Domain):
    system_type: ClassVar[str] = "gcs"
    name: str
    system_kind: str
    sys_path: PurePosixPath
    base_path: PurePosixPath
    prefix: str  # the domain's object-store prefix (== the entity container)
    bucket: str
    service_account_key: str
    # S3-compatible HMAC keys (present in the descriptor since TD-2772); optional so
    # a descriptor without them still constructs. Consumed by connectors that reach
    # GCS over the S3 protocol (e.g. ClickHouse staging's ``s3()`` path).
    hmac_access_key: str | None = None
    hmac_secret_key: str | None = None
    model: str | None = None
    data: str | None = None
    entities: dict[str, int] | None = None  # {table: record count}, once loaded


def gcs_system(name: str):
    return require(name, GCSSystem)


def gcs_domain(name: str):
    return require(name, GCSDomain)
