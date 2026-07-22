#
# Copyright 2026 Tabsdata Inc.
#

from tabsdata.connector.schema.evolution.iceberg.renderers.base import DialectRenderer
from tabsdata.connector.schema.evolution.iceberg.renderers.clickhouse import (
    ClickHouseRenderer,
)
from tabsdata.connector.schema.evolution.iceberg.renderers.starrocks import (
    StarRocksRenderer,
)

DIALECT_RENDERERS: dict[str, DialectRenderer] = {
    "clickhouse": ClickHouseRenderer(),
    "starrocks": StarRocksRenderer(),
}


def render_sql(
    delta,
    dialect: str,
    options=None,
) -> list[str]:
    """Convenience function: look up the dialect renderer and render SQL.

    Args:
        delta: The SchemaDelta to render.
        dialect: Dialect name ("clickhouse", "postgresql", "mysql", "db2").
        options: Render options.

    Returns:
        List of SQL statements.

    Raises:
        ValueError: If the dialect is unknown.
    """
    renderer = DIALECT_RENDERERS.get(dialect)
    if renderer is None:
        raise ValueError(
            f"Unknown dialect: {dialect}. Available: {list(DIALECT_RENDERERS)}"
        )
    return renderer.render_sql(delta, options)
