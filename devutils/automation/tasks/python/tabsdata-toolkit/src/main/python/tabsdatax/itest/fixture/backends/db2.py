#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``db2`` backend."""

from __future__ import annotations

from typing import ClassVar

from pydantic.dataclasses import dataclass

from ..backend import Domain, System, require


@dataclass(frozen=True)
class Db2System(System):
    system_type: ClassVar[str] = "db2"
    name: str
    system_kind: str
    container: str
    image: str
    host: str
    port: str
    user: str
    password: str


@dataclass(frozen=True)
class Db2Domain(Domain):
    system_type: ClassVar[str] = "db2"
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
    schema: str  # db2 calls the entity container a schema (== db here)
    model: str | None = None
    data: str | None = None
    entities: dict[str, int] | None = None  # {table: record count}, once loaded


def db2_system(name: str):
    return require(name, Db2System)


def db2_domain(name: str):
    return require(name, Db2Domain)
