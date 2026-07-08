#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``fs`` (local filesystem) backend.

@fs_system("my_fs")
@fs_domain("inbox")
def test_x(backends):
    my_fs: FsSystem = backends["my_fs"]
    inbox: FsDomain = backends["inbox"]
"""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

from pydantic.dataclasses import dataclass

from ..backend import Domain, System, require


@dataclass(frozen=True)
class FsSystem(System):
    system_type: ClassVar[str] = "fs"
    name: str
    system_kind: str
    sys_path: Path


@dataclass(frozen=True)
class FsDomain(Domain):
    system_type: ClassVar[str] = "fs"
    name: str
    system_kind: str
    sys_path: Path
    base_path: Path
    folder: str  # the domain's directory name (== the entity container)
    model: str | None = None
    data: str | None = None
    entities: dict[str, int] | None = None  # {table: record count}, once loaded


def fs_system(name: str):
    return require(name, FsSystem)


def fs_domain(name: str):
    return require(name, FsDomain)
