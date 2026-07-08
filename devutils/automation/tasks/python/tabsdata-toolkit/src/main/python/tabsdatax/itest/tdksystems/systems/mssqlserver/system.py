#
# Copyright 2026 Tabsdata Inc.
#

"""Microsoft SQL Server. Single-DB-with-login domain model."""

from __future__ import annotations

import os

from ...shell import docker_exec_check, docker_run, gen_password
from .._container import AdminConn, ContainerBase, DomainResult


class MssqlSystem(ContainerBase):
    slug = "mssqlserver"
    label = "Microsoft SQL Server"
    domain_desc = "a database (with a dedicated user that owns it)"
    domain_flag = "schema"
    supports_sql = True
    # CREATE DATABASE can't run inside a multi-statement transaction.
    admin_autocommit = True

    image = (
        "mcr.microsoft.com/mssql/server:"
        f"{os.environ.get('MSSQL_IMAGE_VERSION', '2022-CU14-ubuntu-22.04')}"
    )
    image_platform = "linux/amd64"
    internal_port = 1433
    system_user = "sa"
    password_env_name = "MSSQL_SA_PASSWORD"
    extra_env: list[tuple[str, str]] = [("ACCEPT_EULA", "Y")]
    extra_flags: list[str] = []

    def _docker_run(self, container: str, host_port: int, password: str) -> str:
        return docker_run(self._build_docker_args(container, host_port, password))

    def check_ready(self, container: str, password: str) -> bool:
        return docker_exec_check(
            container,
            "/opt/mssql-tools18/bin/sqlcmd",
            "-S",
            "localhost",
            "-U",
            self.system_user,
            "-P",
            password,
            "-Q",
            "SELECT 1",
            "-C",
            "-N",
            "-l",
            "5",
            "-b",
        )

    def _admin_url(self, admin: AdminConn) -> str:
        return (
            f"mssql+pyodbc://{admin.user}:{admin.password}"
            f"@{admin.host}:{admin.port}/master"
            f"?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        )

    def do_create(self, admin: AdminConn, name: str) -> DomainResult:
        pw = gen_password()
        # No GO batches over pyodbc — each statement runs on its own; USE
        # switches the connection's DB context for the CREATE USER that follows.
        self._run_admin_sql(
            admin,
            [
                f"CREATE DATABASE [{name}]",
                f"CREATE LOGIN [{name}] WITH PASSWORD = '{pw}', CHECK_POLICY = OFF",
                f"USE [{name}]",
                f"CREATE USER [{name}] FOR LOGIN [{name}]",
                f"ALTER ROLE db_owner ADD MEMBER [{name}]",
            ],
        )
        uri = (
            f"mssql+pyodbc://{name}:{pw}@{admin.host}:{admin.port}/{name}"
            f"?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        )
        return DomainResult(db=name, user=name, password=pw, sqlalchemy_uri=uri)

    def do_delete(self, admin: AdminConn, name: str) -> None:
        self._run_admin_sql(
            admin,
            [
                f"ALTER DATABASE [{name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE",
                f"DROP DATABASE [{name}]",
                f"DROP LOGIN [{name}]",
            ],
        )

    def do_list(self, admin: AdminConn) -> list[str]:
        return self._query_admin_names(
            admin,
            (
                "SELECT name FROM sys.databases "
                "WHERE name NOT IN ('master','tempdb','model','msdb') ORDER BY name"
            ),
        )
