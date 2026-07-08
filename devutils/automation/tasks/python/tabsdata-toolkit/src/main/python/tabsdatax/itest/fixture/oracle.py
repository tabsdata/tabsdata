"""Fixture dataclasses for the ``oracle`` backend."""
from __future__ import annotations

from typing import ClassVar

from pydantic.dataclasses import dataclass

from .backend import Domain, System, require


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
    sqlalchemy_uri: str
    model: str | None = None
    data: str | None = None


def oracle_system(name: str):
    return require(name, OracleSystem)


def oracle_domain(name: str):
    return require(name, OracleDomain)
