#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``dev`` (no-op) backend.

@dev_system("my_dev")
@dev_domain("scratch")
def test_x(backends):
    my_dev: DevSystem = backends["my_dev"]
    scratch: DevDomain = backends["scratch"]

The dev backend has no external resource and no connection settings — the
fixtures carry only the identity keys every descriptor stamps."""

from __future__ import annotations

from typing import ClassVar

from pydantic.dataclasses import dataclass

from ..backend import Domain, System, require


@dataclass(frozen=True)
class DevSystem(System):
    system_type: ClassVar[str] = "dev"
    name: str
    system_kind: str


@dataclass(frozen=True)
class DevDomain(Domain):
    system_type: ClassVar[str] = "dev"
    name: str
    system_kind: str
    model: str | None = None
    data: str | None = None
    entities: dict[str, int] | None = None  # {table: record count}, once loaded


def dev_system(name: str):
    return require(name, DevSystem)


def dev_domain(name: str):
    return require(name, DevDomain)
