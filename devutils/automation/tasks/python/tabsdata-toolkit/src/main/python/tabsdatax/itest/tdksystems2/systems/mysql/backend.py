#
# Copyright 2026 Tabsdata Inc.
#

"""MySQL backend — a docker-run container with a per-domain database.

Only the MySQL dialect hooks live here; the container lifecycle, TCP admin and
data plane come from :class:`SqlContainerBackend`. A domain is a database plus a
dedicated owner user, provisioned over TCP on the published host port (no
``docker exec``); all admin runs as ``root`` over SQLAlchemy. Binlog is on by
default (ROW + FULL metadata) so the same container feeds CDC.
"""

from __future__ import annotations

from tabsdatax.itest.tdksystems2.systems._common import container_info
from tabsdatax.itest.tdksystems2.systems._common.sql import SqlContainerBackend


class Mysql(SqlContainerBackend):
    slug = "mysql"
    label = "MySQL"
    domain_word = "database"
    max_entity_name_length = 64  # MySQL database name limit

    image = container_info.MYSQL.ref
    image_platform = container_info.MYSQL.platform
    internal_port = 3306
    root_user = "root"
    driver = "mysql+mysqlconnector"
    sql_quote = "`"
    cmd_args = [
        "--server-id=1",
        "--log-bin=mysql-bin",
        "--binlog-format=ROW",
        "--binlog-row-metadata=FULL",
        "--binlog-expire-logs-seconds=259200",
    ]
    system_schemas = frozenset({
        "information_schema",
        "mysql",
        "performance_schema",
        "sys",
    })

    def env_vars(self, password: str) -> dict[str, str]:
        return {"MYSQL_ROOT_PASSWORD": password}

    def ready_exec_cmd(self, password: str) -> list[str]:
        return [
            "mysqladmin",
            "ping",
            "-h",
            "localhost",
            f"-u{self.root_user}",
            f"--password={password}",
            "--silent",
        ]

    def create_domain_sql(self, db: str, user: str, password: str) -> list[str]:
        return [
            f"CREATE DATABASE `{db}`",
            f"CREATE USER '{user}'@'%' IDENTIFIED BY '{password}'",
            f"GRANT ALL PRIVILEGES ON `{db}`.* TO '{user}'@'%'",
            f"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{user}'@'%'",
            "FLUSH PRIVILEGES",
        ]

    def delete_domain_sql(self, name: str) -> list[str]:
        return [
            f"DROP DATABASE IF EXISTS `{name}`",
            f"DROP USER IF EXISTS '{self._login(name)}'@'%'",
            "FLUSH PRIVILEGES",
        ]

    def list_schemas_sql(self) -> str:
        return "SELECT schema_name FROM information_schema.schemata"
