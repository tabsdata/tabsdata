#
# Copyright 2026 Tabs Data Inc.
#

"""Backend fixtures for database, vault, and cloud storage testing."""

from src.utest.python.common.backend.azure import azure_client, azure_config
from src.utest.python.common.backend.gcs import gcs_client, gcs_config
from src.utest.python.common.backend.hashicorp import (
    DEFAULT_PYTEST_HASHICORP_DOCKER_CONTAINER_NAME,
    HASHICORP_PORT,
    HASHICORP_TESTING_SECRET_NAME,
    HASHICORP_TESTING_SECRET_PATH,
    HASHICORP_TESTING_SECRET_VALUE,
    HASHICORP_TESTING_TOKEN,
    HASHICORP_TESTING_URL,
    testing_hashicorp_vault,
)
from src.utest.python.common.backend.kafka import (
    DEFAULT_PYTEST_KAFKA_DOCKER_CONTAINER_NAME,
    KAFKA_ADMIN_PASSWORD,
    KAFKA_ADMIN_USER,
    VALID_KAFKA_PREFIXES,
    KafkaConfig,
    kafka_config,
)
from src.utest.python.common.backend.mariadb import (
    DEFAULT_PYTEST_MARIADB_DOCKER_CONTAINER_NAME,
    MARIADB_FORWARDED_PORT,
    MARIADB_PORT,
    VALID_MARIADB_PREFIXES,
    MariaDBConfig,
    mariadb_config,
)
from src.utest.python.common.backend.mysql import (
    DEFAULT_PYTEST_MYSQL_DOCKER_CONTAINER_NAME,
    MYSQL_FORWARDED_PORT,
    VALID_MYSQL_PREFIXES,
    MySQLConfig,
    mysql_config,
)
from src.utest.python.common.backend.oracle import (
    DEFAULT_PYTEST_ORACLE_DOCKER_CONTAINER_NAME,
    ORACLE_DB_NAME,
    ORACLE_DB_USER,
    ORACLE_FORWARDED_PORT,
    ORACLE_PORT,
    VALID_ORACLE_PREFIXES,
    OracleConfig,
    oracle_config,
)
from src.utest.python.common.backend.postgres import (
    DEFAULT_PYTEST_POSTGRES_DOCKER_CONTAINER_NAME,
    POSTGRES_FORWARDED_PORT,
    POSTGRES_PORT,
    VALID_POSTGRES_PREFIXES,
    PostgresConfig,
    postgres_config,
)
from src.utest.python.common.backend.s3 import s3_client, s3_config
from src.utest.python.common.constants import (
    DB_HOST,
    DB_NAME,
    DB_PASSWORD,
    DB_USER,
    DOCKER_REGULAR_CONTAINER_INFIX,
    MAXIMUM_RETRY_COUNT,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
)
from src.utest.python.common.container import get_container_port_binding

__all__ = [
    # Shared constants
    "DB_HOST",
    "DB_NAME",
    "DB_PASSWORD",
    "DB_USER",
    "DOCKER_REGULAR_CONTAINER_INFIX",
    "MAXIMUM_RETRY_COUNT",
    "PYTEST_DEFAULT_ENVIRONMENT_PREFIX",
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
