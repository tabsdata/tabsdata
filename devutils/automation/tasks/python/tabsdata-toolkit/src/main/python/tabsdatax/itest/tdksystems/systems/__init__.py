#
# Copyright 2026 Tabsdata Inc.
#

"""Unified system registry. ONE slug->System map replaces the three
per-category `_SLUGS` dicts (container/cloud/saas). A new system is a new
`systems/<slug>/` directory plus one line here.
"""

from __future__ import annotations

from importlib import import_module

from ..errors import DatagenError

_SLUGS = {
    # container
    "mysql": ("mysql.system", "MysqlSystem"),
    "mysqlcdc": ("mysqlcdc.system", "MysqlCdcSystem"),
    "postgres": ("postgres.system", "PostgresSystem"),
    "postgrescdc": ("postgrescdc.system", "PostgresCdcSystem"),
    "mssqlserver": ("mssqlserver.system", "MssqlSystem"),
    "oracle": ("oracle.system", "OracleSystem"),
    "db2": ("db2.system", "Db2System"),
    "db2cdc": ("db2cdc.system", "Db2CdcSystem"),
    "clickhouse": ("clickhouse.system", "ClickhouseSystem"),
    "mongodb": ("mongodb.system", "MongoDBSystem"),
    "starrocks": ("starrocks.system", "StarrocksSystem"),
    "redpanda": ("redpanda.system", "RedpandaSystem"),
    # containers (compose stack)
    "druid": ("druid.system", "DruidSystem"),
    # cloud / fs
    "s3": ("s3.system", "S3System"),
    "gcs": ("gcs.system", "GCSSystem"),
    "azureblob": ("azureblob.system", "AzureblobSystem"),
    "fs": ("fs.system", "FsSystem"),
    # saas
    "databricks": ("databricks.system", "DatabricksSystem"),
    "snowflake": ("snowflake.system", "SnowflakeSystem"),
    "salesforce": ("salesforce.system", "SalesforceSystem"),
    "bigquery": ("bigquery.system", "BigQuerySystem"),
    # service (credential-only API accounts)
    "anthropic": ("anthropic.system", "AnthropicSystem"),
    "openai": ("openai.system", "OpenAISystem"),
    # dev (no-op)
    "dev": ("dev.system", "DevSystem"),
}


def get(slug: str):
    """Instantiate the System for `slug` (category-agnostic)."""
    if slug not in _SLUGS:
        raise DatagenError(
            f"unknown system: {slug} (known: {', '.join(sorted(_SLUGS))})"
        )
    mod_name, cls_name = _SLUGS[slug]
    return getattr(import_module(f".{mod_name}", __name__), cls_name)()


def all_slugs() -> list[str]:
    return sorted(_SLUGS)
