"""Fixture dataclasses for the ``mssqlserver`` backend."""
from __future__ import annotations

from typing import ClassVar

from pydantic.dataclasses import dataclass

from .backend import Domain, System, require


@dataclass(frozen=True)
class MssqlserverSystem(System):
    system_type: ClassVar[str] = "mssqlserver"
    name: str
    system_kind: str
    container: str
    image: str
    host: str
    port: str
    user: str
    password: str


@dataclass(frozen=True)
class MssqlserverDomain(Domain):
    system_type: ClassVar[str] = "mssqlserver"
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


def mssqlserver_system(name: str):
    return require(name, MssqlserverSystem)


def mssqlserver_domain(name: str):
    return require(name, MssqlserverDomain)
