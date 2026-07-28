#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``db2cdc`` backend."""

from __future__ import annotations

from typing import ClassVar

from pydantic.dataclasses import dataclass

from ..backend import Domain, System, require


@dataclass(frozen=True)
class Db2CdcSystem(System):
    system_type: ClassVar[str] = "db2cdc"
    name: str
    system_kind: str
    container: str
    image: str
    host: str
    port: str
    user: str
    password: str


@dataclass(frozen=True)
class Db2CdcDomain(Domain):
    system_type: ClassVar[str] = "db2cdc"
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
    # The container the capture program runs in. Unlike mysql's binlog or
    # postgres' logical slots, Db2 capture is an OS process (``asncap``) reading
    # archived logs, so a test that needs to interrogate or nudge it has to reach
    # into the container.
    sys_container: str | None = None
    model: str | None = None
    data: str | None = None
    entities: dict[str, int] | None = None  # {table: record count}, once loaded


def db2cdc_system(name: str):
    return require(name, Db2CdcSystem)


def db2cdc_domain(name: str):
    return require(name, Db2CdcDomain)
