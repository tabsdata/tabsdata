#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``salesforce`` backend."""

from __future__ import annotations

from typing import ClassVar

from pydantic.dataclasses import dataclass

from ..backend import Domain, System, require


@dataclass(frozen=True)
class SalesforceSystem(System):
    system_type: ClassVar[str] = "salesforce"
    name: str
    system_kind: str
    url: str
    username: str
    password: str
    token: str


@dataclass(frozen=True)
class SalesforceDomain(Domain):
    system_type: ClassVar[str] = "salesforce"
    name: str
    system_kind: str
    url: str
    username: str
    password: str
    token: str
    model: str | None = None
    data: str | None = None


def salesforce_system(name: str):
    return require(name, SalesforceSystem)


def salesforce_domain(name: str):
    return require(name, SalesforceDomain)
