#
# Copyright 2026 Tabsdata Inc.
#

"""System registry.

A system is a ``systems/<slug>/`` package whose ``__init__`` exposes the backend
class. The backend's capability flags (``supports_models`` / ``supports_data`` /
``supports_changes`` / ``is_container`` / ``supports_purge``) decide which verbs
the tool exposes for it; each verb runs the shared implementation in
``_common/commands`` unless the system ships its own ``<verb>.py`` override. A new
system is a new directory here plus one line in ``_SLUGS`` (and, for a SQL
database, per-model ``domain/models/<model>/<slug>.ddl`` files).
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING

from tabsdatax.itest.tdksystems2.errors import TdxError

if TYPE_CHECKING:
    from tabsdatax.itest.tdksystems2.protocols import Backend

# slug -> (package under systems/, backend class name)
_SLUGS: dict[str, tuple[str, str]] = {
    "mysql": ("mysql", "Mysql"),
    "postgres": ("postgres", "Postgres"),
    "mssqlserver": ("mssqlserver", "Mssqlserver"),
    "oracle": ("oracle", "Oracle"),
    "starrocks": ("starrocks", "Starrocks"),
    "db2": ("db2", "Db2"),
    "mysqlcdc": ("mysqlcdc", "Mysqlcdc"),
    "postgrescdc": ("postgrescdc", "Postgrescdc"),
    "fs": ("fs", "Fs"),
    "s3": ("s3", "S3"),
    "gcs": ("gcs", "GCS"),
    "azureblob": ("azureblob", "Azureblob"),
    "anthropic": ("anthropic", "Anthropic"),
    "openai": ("openai", "Openai"),
    "dev": ("dev", "Dev"),
    "snowflake": ("snowflake", "Snowflake"),
    "databricks": ("databricks", "Databricks"),
    "bigquery": ("bigquery", "BigQuery"),
    "mongodb": ("mongodb", "MongoDB"),
    "redpanda": ("redpanda", "Redpanda"),
    "druid": ("druid", "Druid"),
    "clickhouse": ("clickhouse", "Clickhouse"),
    "db2cdc": ("db2cdc", "Db2Cdc"),
    "salesforce": ("salesforce", "Salesforce"),
}


def all_slugs() -> list[str]:
    return sorted(_SLUGS)


def get_class(slug: str) -> type[Backend]:
    """The backend class for `slug` (no instance — used to read capability flags
    when the dispatcher decides which verbs a system exposes)."""
    if slug not in _SLUGS:
        raise TdxError(f"unknown system: {slug} (known: {', '.join(all_slugs())})")
    pkg, cls = _SLUGS[slug]
    module = import_module(f"{__package__}.{pkg}")
    return getattr(module, cls)


def get(slug: str) -> Backend:
    """Instantiate the backend for `slug`."""
    return get_class(slug)()
