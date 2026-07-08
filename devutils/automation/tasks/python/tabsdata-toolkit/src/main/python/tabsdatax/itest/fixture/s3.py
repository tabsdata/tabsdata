"""Fixture dataclasses for the ``s3`` backend."""
from __future__ import annotations

from pathlib import Path
from typing import ClassVar

from pydantic.dataclasses import dataclass

from .backend import Domain, System, require


@dataclass(frozen=True)
class S3System(System):
    system_type: ClassVar[str] = "s3"
    name: str
    system_kind: str
    sys_path: Path
    bucket: str
    region: str
    access_key: str
    secret_key: str


@dataclass(frozen=True)
class S3Domain(Domain):
    system_type: ClassVar[str] = "s3"
    name: str
    system_kind: str
    sys_path: Path
    base_path: Path
    bucket: str
    region: str
    access_key: str
    secret_key: str
    model: str | None = None
    data: str | None = None


def s3_system(name: str):
    return require(name, S3System)


def s3_domain(name: str):
    return require(name, S3Domain)
