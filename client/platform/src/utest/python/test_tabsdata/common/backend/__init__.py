#
# Copyright 2026 Tabs Data Inc.
#

"""Backend fixtures for database, vault, and cloud storage testing."""

from test_tabsdata.common.backend.azure import azure_client, azure_config
from test_tabsdata.common.backend.clickhouse import (
    CLICKHOUSE_FORWARDED_PORT,
    CLICKHOUSE_PORT,
    CLICKHOUSE_DB_USER,
    CLICKHOUSE_DB_NAME,
    DEFAULT_PYTEST_CLICKHOUSE_DOCKER_CONTAINER_NAME,
    VALID_CLICKHOUSE_PREFIXES,
    ClickHouseTestConfig,
    clickhouse_config,
    clickhouse_client,
)
from test_tabsdata.common.backend.druid import (
    DEFAULT_PYTEST_DRUID_DOCKER_CONTAINER_NAME,
    DRUID_OVERLORD_FORWARDED_PORT,
    DRUID_OVERLORD_PORT,
    DRUID_ROUTER_FORWARDED_PORT,
    DRUID_ROUTER_PORT,
    VALID_DRUID_PREFIXES,
    DruidConfig,
    druid_config,
)
from test_tabsdata.common.backend.db2 import (
    DEFAULT_PYTEST_DB2_DOCKER_CONTAINER_NAME,
    DB2_DB_NAME,
    DB2_DB_USER,
    DB2_FORWARDED_PORT,
    DB2_PORT,
    VALID_DB2_PREFIXES,
    DB2Config,
    db2_config,
)
from test_tabsdata.common.backend.gcs import gcs_client, gcs_config
from test_tabsdata.common.backend.hashicorp import (
    DEFAULT_PYTEST_HASHICORP_DOCKER_CONTAINER_NAME,
    HASHICORP_PORT,
    HASHICORP_TESTING_SECRET_NAME,
    HASHICORP_TESTING_SECRET_PATH,
    HASHICORP_TESTING_SECRET_VALUE,
    HASHICORP_TESTING_TOKEN,
    HASHICORP_TESTING_URL,
    testing_hashicorp_vault,
)
from test_tabsdata.common.backend.kafka import (
    DEFAULT_PYTEST_KAFKA_DOCKER_CONTAINER_NAME,
    KAFKA_ADMIN_PASSWORD,
    KAFKA_ADMIN_USER,
    VALID_KAFKA_PREFIXES,
    KafkaConfig,
    kafka_config,
)
from test_tabsdata.common.backend.mariadb import (
    DEFAULT_PYTEST_MARIADB_DOCKER_CONTAINER_NAME,
    MARIADB_FORWARDED_PORT,
    MARIADB_PORT,
    VALID_MARIADB_PREFIXES,
    MariaDBConfig,
    mariadb_config,
)
from test_tabsdata.common.backend.mysql import (
    DEFAULT_PYTEST_MYSQL_DOCKER_CONTAINER_NAME,
    MYSQL_FORWARDED_PORT,
    VALID_MYSQL_PREFIXES,
    MySQLConfig,
    mysql_config,
)
from test_tabsdata.common.backend.oracle import (
    DEFAULT_PYTEST_ORACLE_DOCKER_CONTAINER_NAME,
    ORACLE_DB_NAME,
    ORACLE_DB_USER,
    ORACLE_FORWARDED_PORT,
    ORACLE_PORT,
    VALID_ORACLE_PREFIXES,
    OracleConfig,
    oracle_config,
)
from test_tabsdata.common.backend.postgres import (
    DEFAULT_PYTEST_POSTGRES_DOCKER_CONTAINER_NAME,
    POSTGRES_FORWARDED_PORT,
    POSTGRES_PORT,
    VALID_POSTGRES_PREFIXES,
    PostgresConfig,
    postgres_config,
)
from test_tabsdata.common.backend.s3 import s3_client, s3_config
from test_tabsdata.common.backend.starrocks import (
    DEFAULT_PYTEST_STARROCKS_DOCKER_CONTAINER_NAME,
    STARROCKS_DB_NAME,
    STARROCKS_DB_USER,
    STARROCKS_MYSQL_FORWARDED_PORT,
    STARROCKS_MYSQL_PORT,
    VALID_STARROCKS_PREFIXES,
    StarRocksTestConfig,
    starrocks_config,
    starrocks_client,
)
from test_tabsdata.common.constants import (
    DB_HOST,
    DB_NAME,
    DB_PASSWORD,
    DB_USER,
    DOCKER_REGULAR_CONTAINER_INFIX,
    CONTAINER_MAXIMUM_RETRY_COUNT,
    EXECUTION_MAXIMUM_RETRY_COUNT,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
)
from test_tabsdata.common.container import get_container_port_binding

__all__ = [
    # Shared constants
    "DB_HOST",
    "DB_NAME",
    "DB_PASSWORD",
    "DB_USER",
    "DOCKER_REGULAR_CONTAINER_INFIX",
    "CONTAINER_MAXIMUM_RETRY_COUNT",
    "EXECUTION_MAXIMUM_RETRY_COUNT",
    "PYTEST_DEFAULT_ENVIRONMENT_PREFIX",
    # ClickHouse
    "DEFAULT_PYTEST_CLICKHOUSE_DOCKER_CONTAINER_NAME",
    "CLICKHOUSE_DB_USER",
    "CLICKHOUSE_DB_NAME",
    "CLICKHOUSE_FORWARDED_PORT",
    "CLICKHOUSE_PORT",
    "VALID_CLICKHOUSE_PREFIXES",
    "ClickHouseTestConfig",
    "clickhouse_config",
    "clickhouse_client",
    # HashiCorp
    "DEFAULT_PYTEST_HASHICORP_DOCKER_CONTAINER_NAME",
    "HASHICORP_PORT",
    "HASHICORP_TESTING_SECRET_NAME",
    "HASHICORP_TESTING_SECRET_PATH",
    "HASHICORP_TESTING_SECRET_VALUE",
    "HASHICORP_TESTING_TOKEN",
    "HASHICORP_TESTING_URL",
    "testing_hashicorp_vault",
    # Kafka
    "DEFAULT_PYTEST_KAFKA_DOCKER_CONTAINER_NAME",
    "KAFKA_ADMIN_PASSWORD",
    "KAFKA_ADMIN_USER",
    "KafkaConfig",
    "VALID_KAFKA_PREFIXES",
    "kafka_config",
    # Druid
    "DEFAULT_PYTEST_DRUID_DOCKER_CONTAINER_NAME",
    "DRUID_OVERLORD_FORWARDED_PORT",
    "DRUID_OVERLORD_PORT",
    "DRUID_ROUTER_FORWARDED_PORT",
    "DRUID_ROUTER_PORT",
    "VALID_DRUID_PREFIXES",
    "DruidConfig",
    "druid_config",
    # DB2
    "DEFAULT_PYTEST_DB2_DOCKER_CONTAINER_NAME",
    "DB2_DB_NAME",
    "DB2_DB_USER",
    "DB2_FORWARDED_PORT",
    "DB2_PORT",
    "VALID_DB2_PREFIXES",
    "DB2Config",
    "db2_config",
    # MariaDB
    "DEFAULT_PYTEST_MARIADB_DOCKER_CONTAINER_NAME",
    "MARIADB_FORWARDED_PORT",
    "MARIADB_PORT",
    "MariaDBConfig",
    "VALID_MARIADB_PREFIXES",
    "mariadb_config",
    # MySQL
    "DEFAULT_PYTEST_MYSQL_DOCKER_CONTAINER_NAME",
    "MYSQL_FORWARDED_PORT",
    "MySQLConfig",
    "VALID_MYSQL_PREFIXES",
    "mysql_config",
    # Oracle
    "DEFAULT_PYTEST_ORACLE_DOCKER_CONTAINER_NAME",
    "ORACLE_DB_NAME",
    "ORACLE_DB_USER",
    "ORACLE_FORWARDED_PORT",
    "ORACLE_PORT",
    "OracleConfig",
    "VALID_ORACLE_PREFIXES",
    "oracle_config",
    # PostgreSQL
    "DEFAULT_PYTEST_POSTGRES_DOCKER_CONTAINER_NAME",
    "POSTGRES_FORWARDED_PORT",
    "POSTGRES_PORT",
    "PostgresConfig",
    "VALID_POSTGRES_PREFIXES",
    "postgres_config",
    # StarRocks
    "DEFAULT_PYTEST_STARROCKS_DOCKER_CONTAINER_NAME",
    "STARROCKS_DB_NAME",
    "STARROCKS_DB_USER",
    "STARROCKS_MYSQL_FORWARDED_PORT",
    "STARROCKS_MYSQL_PORT",
    "VALID_STARROCKS_PREFIXES",
    "StarRocksTestConfig",
    "starrocks_config",
    "starrocks_client",
    # Cloud Storage
    "azure_client",
    "azure_config",
    "gcs_client",
    "gcs_config",
    "s3_client",
    "s3_config",
    # Helper functions
    "get_container_port_binding",
]
