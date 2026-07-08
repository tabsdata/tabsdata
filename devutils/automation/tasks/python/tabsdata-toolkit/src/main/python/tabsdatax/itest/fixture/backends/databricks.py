#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``databricks`` backend."""

from __future__ import annotations

from typing import ClassVar

from pydantic.dataclasses import dataclass

from ..backend import Domain, System, require


@dataclass(frozen=True)
class DatabricksSystem(System):
    system_type: ClassVar[str] = "databricks"
    name: str
    system_kind: str
    host: str
    token: str
    warehouse_id: str
    catalog: str
    volume: str


@dataclass(frozen=True)
class DatabricksDomain(Domain):
    system_type: ClassVar[str] = "databricks"
    name: str
    system_kind: str
    schema: str
    host: str
    token: str
    warehouse_id: str
    catalog: str
    volume: str
    model: str | None = None
    data: str | None = None
    entities: dict[str, int] | None = None  # {table: record count}, once loaded


def databricks_system(name: str):
    return require(name, DatabricksSystem)


def databricks_domain(name: str):
    return require(name, DatabricksDomain)
