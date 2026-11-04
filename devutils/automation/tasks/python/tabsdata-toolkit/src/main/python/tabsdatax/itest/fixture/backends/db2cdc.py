#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``db2cdc`` backend."""

from __future__ import annotations

from typing import ClassVar

from pydantic.dataclasses import dataclass

from tabsdatax.itest.fixture import operations
from tabsdatax.itest.fixture.backend import Domain, System, require


@dataclass(frozen=True)
class Db2CdcSystem(System):
    system_type: ClassVar[str] = "db2cdc"
    name: str
    system_kind: str
    docker_container_name: str
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
    sys_docker_container_name: str
    model: str | None = None
    data: str | None = None
    entities: dict[str, int] | None = None  # {table: record count}, once loaded

    def capture_table(self, *, table: str, ddl: str | None = None) -> str:
        """Register `table` of this domain for change capture via ``tdd domain
        capture-table``, creating it from `ddl` first when given; returns the CD
        table capture writes its changes into."""
        return operations.capture_table(self.name, table=table, ddl=ddl)

    def release_table(self, *, table: str, drop: bool = False) -> None:
        """Undo :meth:`capture_table` via ``tdd domain release-table``, dropping
        the table too when `drop`."""
        operations.release_table(self.name, table=table, drop=drop)


def db2cdc_system(name: str):
    return require(name, Db2CdcSystem)


def db2cdc_domain(name: str):
    return require(name, Db2CdcDomain)
