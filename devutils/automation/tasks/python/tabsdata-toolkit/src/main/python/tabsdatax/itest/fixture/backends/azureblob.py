#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``azureblob`` backend."""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

from pydantic.dataclasses import dataclass

from ..backend import Domain, System, require


@dataclass(frozen=True)
class AzureblobSystem(System):
    system_type: ClassVar[str] = "azureblob"
    name: str
    system_kind: str
    sys_path: Path
    container: str
    account_name: str
    account_key: str


@dataclass(frozen=True)
class AzureblobDomain(Domain):
    system_type: ClassVar[str] = "azureblob"
    name: str
    system_kind: str
    sys_path: Path
    base_path: Path
    container: str
    account_name: str
    account_key: str
    model: str | None = None
    data: str | None = None


def azureblob_system(name: str):
    return require(name, AzureblobSystem)


def azureblob_domain(name: str):
    return require(name, AzureblobDomain)
