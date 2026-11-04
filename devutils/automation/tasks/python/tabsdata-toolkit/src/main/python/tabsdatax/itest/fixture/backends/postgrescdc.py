#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``postgrescdc`` backend."""

from __future__ import annotations

from typing import ClassVar

from pydantic.dataclasses import dataclass

from tabsdatax.itest.fixture.backend import Domain, System, require


@dataclass(frozen=True)
class PostgrescdcSystem(System):
    system_type: ClassVar[str] = "postgrescdc"
    name: str
    system_kind: str
    docker_container_name: str
    image: str
    external_host: str
    port: str
    user: str
    password: str


@dataclass(frozen=True)
class PostgrescdcDomain(Domain):
    system_type: ClassVar[str] = "postgrescdc"
    name: str
    system_kind: str
    image: str
    external_host: str
    port: str
    user: str
    password: str
    database: str
    external_uri: str  # credential-less; creds are supplied separately as UserPassword
    external_sqlalchemy_uri: str
    schema: str  # postgres calls the entity container a schema (== database here)
    model: str | None = None
    data: str | None = None
    entities: dict[str, int] | None = None  # {table: record count}, once loaded


def postgrescdc_system(name: str):
    return require(name, PostgrescdcSystem)


def postgrescdc_domain(name: str):
    return require(name, PostgrescdcDomain)
