#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``redpanda`` backend."""

from __future__ import annotations

from typing import ClassVar

from pydantic.dataclasses import dataclass

from ..backend import Domain, System, require


@dataclass(frozen=True)
class RedpandaSystem(System):
    system_type: ClassVar[str] = "redpanda"
    name: str
    system_kind: str
    container: str
    image: str
    host: str
    port: str
    user: str
    password: str


@dataclass(frozen=True)
class RedpandaDomain(Domain):
    system_type: ClassVar[str] = "redpanda"
    name: str
    system_kind: str
    image: str
    host: str
    port: str
    user: str
    password: str
    db: str
    sqlalchemy_uri: str
    topic: str  # redpanda calls the entity container a topic (== db here)
    group_id: str  # consumer group id — the build identity (see GROUP_ID)
    model: str | None = None
    data: str | None = None
    entities: dict[str, int] | None = None  # {table: record count}, once loaded


def redpanda_system(name: str):
    return require(name, RedpandaSystem)


def redpanda_domain(name: str):
    return require(name, RedpandaDomain)
