#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``postgres`` backend."""

from __future__ import annotations

from typing import ClassVar

from pydantic.dataclasses import dataclass

from ..backend import Domain, System, require


@dataclass(frozen=True)
class PostgresSystem(System):
    system_type: ClassVar[str] = "postgres"
    name: str
    system_kind: str
    container: str
    image: str
    host: str
    port: str
    user: str
    password: str


@dataclass(frozen=True)
class PostgresDomain(Domain):
    system_type: ClassVar[str] = "postgres"
    name: str
    system_kind: str
    image: str
    host: str
    port: str
    user: str
    password: str
    db: str
    sqlalchemy_uri: str
    model: str | None = None
    data: str | None = None


def postgres_system(name: str):
    return require(name, PostgresSystem)


def postgres_domain(name: str):
    return require(name, PostgresDomain)
