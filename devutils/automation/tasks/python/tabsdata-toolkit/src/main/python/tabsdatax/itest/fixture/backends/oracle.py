#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``oracle`` backend."""

from __future__ import annotations

from typing import ClassVar

from pydantic.dataclasses import dataclass

from ..backend import Domain, System, require


@dataclass(frozen=True)
class OracleSystem(System):
    system_type: ClassVar[str] = "oracle"
    name: str
    system_kind: str
    container: str
    image: str
    host: str
    port: str
    user: str
    password: str


@dataclass(frozen=True)
class OracleDomain(Domain):
    system_type: ClassVar[str] = "oracle"
    name: str
    system_kind: str
    image: str
    host: str
    port: str
    user: str
    password: str
    db: str
    uri: str  # credential-less; creds are supplied separately as UserPassword
    sqlalchemy_uri: str
    database: str  # oracle calls the entity container a database (== db here)
    model: str | None = None
    data: str | None = None
    entities: dict[str, int] | None = None  # {table: record count}, once loaded


def oracle_system(name: str):
    return require(name, OracleSystem)


def oracle_domain(name: str):
    return require(name, OracleDomain)
