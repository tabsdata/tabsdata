"""Fixture dataclasses for the ``mysql`` backend."""
from __future__ import annotations

from typing import ClassVar

from pydantic.dataclasses import dataclass

from .backend import Domain, System, require


@dataclass(frozen=True)
class MysqlSystem(System):
    system_type: ClassVar[str] = "mysql"
    name: str
    system_kind: str
    container: str
    image: str
    host: str
    port: str
    user: str
    password: str


@dataclass(frozen=True)
class MysqlDomain(Domain):
    system_type: ClassVar[str] = "mysql"
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


def mysql_system(name: str):
    return require(name, MysqlSystem)


def mysql_domain(name: str):
    return require(name, MysqlDomain)
