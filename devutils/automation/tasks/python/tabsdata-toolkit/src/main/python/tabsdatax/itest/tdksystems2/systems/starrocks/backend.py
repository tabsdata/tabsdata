#
# Copyright 2026 Tabsdata Inc.
#

"""StarRocks backend — the all-in-one image, MySQL-protocol on 9030.

A domain is a database with a dedicated user. The allin1 image mandates the
empty ``root`` password and has a single BE, so replication is tuned to 1 after
start. Admin speaks the MySQL protocol (mysqlconnector). Only the StarRocks
dialect hooks live here.
"""

from __future__ import annotations

import time

from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.shell import docker_exec, docker_exec_check
from tabsdatax.itest.tdksystems2.systems._common import container_info
from tabsdatax.itest.tdksystems2.systems._common.sql import SqlContainerBackend


class Starrocks(SqlContainerBackend):
    slug = "starrocks"
    label = "StarRocks"
    weight = 2  # heavy all-in-one image — schedule ahead of the light systems
    domain_word = "database"
    max_entity_name_length = 256

    image = container_info.STARROCKS.ref
    image_platform = container_info.STARROCKS.platform
    internal_port = 9030
    root_user = "root"
    driver = "mysql+mysqlconnector"
    sql_quote = "`"
    system_schemas = frozenset({"information_schema", "_statistics_", "sys"})

    def generate_password(self) -> str:
        return ""  # allin1 requires the empty root password

    def env_vars(self, password: str) -> dict[str, str]:
        return {}  # no root-password env

    def _mysql(self, container: str, *sql_args: str, sql: str | None = None):
        return docker_exec(
            container,
            "mysql",
            "-h",
            "127.0.0.1",
            "-P",
            "9030",
            "-u",
            "root",
            "--password=",
            *sql_args,
            input=sql,
        )

    def check_ready(self, container: str, password: str) -> bool:
        # FE accepts SELECT 1 AND every BE reports Alive=true.
        if not docker_exec_check(
            container,
            "mysql",
            "-h",
            "127.0.0.1",
            "-P",
            "9030",
            "-u",
            "root",
            "--password=",
            "-e",
            "SELECT 1",
        ):
            return False
        cp = self._mysql(container, "-BNe", "SHOW BACKENDS")
        if cp.returncode != 0 or not cp.stdout.strip():
            return False
        for line in cp.stdout.strip().splitlines():
            cols = line.split("\t")
            if len(cols) < 9 or cols[8] != "true":
                return False
        return True

    def post_provision(self, container: str, password: str) -> None:
        # allin1 has one BE; the default replication=3 makes every CREATE TABLE
        # fail. Wait briefly for the FE to accept the admin config command.
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            cp = self._mysql(
                container,
                sql='admin set frontend config ("default_replication_num" = "1");\n',
            )
            if cp.returncode == 0:
                return
            time.sleep(2)
        raise TdxError("could not set default_replication_num=1 on StarRocks")

    def create_domain_sql(self, db: str, user: str, password: str) -> list[str]:
        # StarRocks RBAC: CREATE TABLE is a DATABASE privilege, DML a TABLE one;
        # ON ALL TABLES IN DATABASE is a standing grant over current+future tables.
        return [
            f"CREATE DATABASE `{db}`",
            f"CREATE USER '{user}'@'%' IDENTIFIED BY '{password}'",
            f"GRANT ALL ON DATABASE `{db}` TO '{user}'@'%'",
            f"GRANT ALL ON ALL TABLES IN DATABASE `{db}` TO '{user}'@'%'",
        ]

    def delete_domain_sql(self, name: str) -> list[str]:
        return [
            f"DROP DATABASE IF EXISTS `{name}`",
            f"DROP USER IF EXISTS '{self._login(name)}'@'%'",
        ]

    def list_schemas_sql(self) -> str:
        return (
            "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name"
        )
