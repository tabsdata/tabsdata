"""Fixture dataclasses for the ``gcs`` backend."""
from __future__ import annotations

from pathlib import Path
from typing import ClassVar

from pydantic.dataclasses import dataclass

from .backend import Domain, System, require


@dataclass(frozen=True)
class GcsSystem(System):
    system_type: ClassVar[str] = "gcs"
    name: str
    system_kind: str
    sys_path: Path
    bucket: str
    service_account_key: str


@dataclass(frozen=True)
class GcsDomain(Domain):
    system_type: ClassVar[str] = "gcs"
    name: str
    system_kind: str
    sys_path: Path
    base_path: Path
    bucket: str
    service_account_key: str
    model: str | None = None
    data: str | None = None


def gcs_system(name: str):
    return require(name, GcsSystem)


def gcs_domain(name: str):
    return require(name, GcsDomain)
