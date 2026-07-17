#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``druid`` backend (a compose stack; domain == a
datasource)."""

from __future__ import annotations

from typing import ClassVar

from pydantic.dataclasses import dataclass

from ..backend import Domain, System, require


@dataclass(frozen=True)
class DruidSystem(System):
    system_type: ClassVar[str] = "druid"
    name: str
    system_kind: str
    project: str
    image: str
    overlord_url: str
    router_url: str
    user: str
    password: str


@dataclass(frozen=True)
class DruidDomain(Domain):
    system_type: ClassVar[str] = "druid"
    name: str
    system_kind: str
    datasource: str
    overlord_url: str
    router_url: str
    user: str
    password: str
    entities: dict[str, int] | None = None  # {datasource: record count}, once loaded


def druid_system(name: str):
    return require(name, DruidSystem)


def druid_domain(name: str):
    return require(name, DruidDomain)
