"""Fixture dataclasses for the ``mongodb`` backend."""
from __future__ import annotations

from typing import ClassVar

from pydantic.dataclasses import dataclass

from .backend import Domain, System, require


@dataclass(frozen=True)
class MongodbSystem(System):
    system_type: ClassVar[str] = "mongodb"
    name: str
    system_kind: str
    container: str
    image: str
    host: str
    port: str
    user: str
    password: str


@dataclass(frozen=True)
class MongodbDomain(Domain):
    system_type: ClassVar[str] = "mongodb"
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


def mongodb_system(name: str):
    return require(name, MongodbSystem)


def mongodb_domain(name: str):
    return require(name, MongodbDomain)
