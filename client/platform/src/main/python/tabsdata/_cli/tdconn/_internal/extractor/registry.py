#
#  Copyright 2026. Tabs Data Inc.
#
"""Mapping from Conn class name to the extractor that understands it.

Used by :meth:`Logic.verify` (and any other caller that starts from a
stored connection's ``conn_type`` string and needs to invoke the
matching :class:`ConnectionExtractor`).

Kept as an explicit table rather than discovered via ``__subclasses__``
so the set of supported connectors is grep-able and obvious: if a new
Conn subclass lands without a corresponding entry here, ``verify``
will surface that as a clear ``ValueError`` rather than silently
skipping it.
"""

from __future__ import annotations

from tabsdata._cli.tdconn._internal.extractor.api import ConnectionExtractor
from tabsdata._cli.tdconn._internal.extractor.connector.azure.extractor import (
    AzureConnectionExtractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.bigquery.extractor import (
    BigQueryConnectionExtractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.cdc.db2.extractor import (
    Db2CdcConnectionExtractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.cdc.mysql.extractor import (
    MySQLCdcConnectionExtractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.cdc.postgres.extractor import (
    PostgresCdcConnectionExtractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.clickhouse.extractor import (
    ClickHouseConnectionExtractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.databricks.extractor import (
    DatabricksConnectionExtractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.db2.extractor import (
    Db2ConnectionExtractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.druid.extractor import (
    DruidConnectionExtractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.gcs.extractor import (
    GCSConnectionExtractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.mariadb.extractor import (
    MariaDbConnectionExtractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.mongodb.extractor import (
    MongoDBConnectionExtractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.mssqlserver.extractor import (
    MSSQLServerConnectionExtractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.mysql.extractor import (
    MySQLConnectionExtractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.oracle.extractor import (
    OracleConnectionExtractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.postgres.extractor import (
    PostgresConnectionExtractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.s3.extractor import (
    S3ConnectionExtractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.salesforce import (
    report_extractor,
    soql_extractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.snowflake.extractor import (
    SnowflakeConnectionExtractor,
)
from tabsdata._cli.tdconn._internal.extractor.connector.starrocks.extractor import (
    StarRocksConnectionExtractor,
)

SalesforceReportsConnectionExtractor = (
    report_extractor.SalesforceReportsConnectionExtractor
)
SalesforceSOQLConnectionExtractor = soql_extractor.SalesforceSOQLConnectionExtractor

EXTRACTOR_BY_CONN_TYPE: dict[str, type[ConnectionExtractor]] = {
    # SQL / relational
    "MySQLConn": MySQLConnectionExtractor,
    "MariaDBConn": MariaDbConnectionExtractor,
    "PostgresConn": PostgresConnectionExtractor,
    "OracleConn": OracleConnectionExtractor,
    "Db2Conn": Db2ConnectionExtractor,
    "MSSQLConn": MSSQLServerConnectionExtractor,
    "SnowflakeConn": SnowflakeConnectionExtractor,
    "DatabricksConn": DatabricksConnectionExtractor,
    "BigQueryConn": BigQueryConnectionExtractor,
    "ClickHouseConn": ClickHouseConnectionExtractor,
    "StarRocksConn": StarRocksConnectionExtractor,
    "DruidConn": DruidConnectionExtractor,
    # CDC
    "MySQLCdcConn": MySQLCdcConnectionExtractor,
    "PostgresCdcConn": PostgresCdcConnectionExtractor,
    "Db2CdcConn": Db2CdcConnectionExtractor,
    # Object / document
    "MongoDBConn": MongoDBConnectionExtractor,
    # CRM — two extractors share SalesforceConn; SOQL is the default probe.
    "SalesforceConn": SalesforceSOQLConnectionExtractor,
    # File storage
    "S3Conn": S3ConnectionExtractor,
    "AzureConn": AzureConnectionExtractor,
    "GCSConn": GCSConnectionExtractor,
}


def extractor_for(conn_type: str) -> type[ConnectionExtractor]:
    """Return the extractor class registered for *conn_type*.

    Raises :class:`ValueError` when no extractor is registered — a
    clear signal that a Conn subclass was added without wiring up its
    extractor here.
    """
    cls = EXTRACTOR_BY_CONN_TYPE.get(conn_type)
    if cls is None:
        raise ValueError(f"No extractor registered for conn_type {conn_type!r}")
    return cls


# Also re-export the Reports variant so callers that specifically want
# report metadata (rather than SOQL objects) can bypass the default.
SALESFORCE_REPORTS_EXTRACTOR = SalesforceReportsConnectionExtractor
