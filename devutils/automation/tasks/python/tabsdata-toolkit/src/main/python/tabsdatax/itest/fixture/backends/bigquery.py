#
# Copyright 2026 Tabsdata Inc.
#

"""Fixture dataclasses for the ``bigquery`` backend."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import ClassVar

from pydantic.dataclasses import dataclass

from ..backend import Domain, System, require


@dataclass(frozen=True)
class BigQuerySystem(System):
    system_type: ClassVar[str] = "bigquery"
    name: str
    system_kind: str
    project: str
    service_account_key: str
    bucket: str
    sys_path: PurePosixPath


@dataclass(frozen=True)
class BigQueryDomain(Domain):
    system_type: ClassVar[str] = "bigquery"
    name: str
    system_kind: str
    project: str
    service_account_key: str
    bucket: str
    sys_path: PurePosixPath
    dataset: str  # == the domain name
    base_path: PurePosixPath


def bigquery_system(name: str):
    return require(name, BigQuerySystem)


def bigquery_domain(name: str):
    return require(name, BigQueryDomain)
