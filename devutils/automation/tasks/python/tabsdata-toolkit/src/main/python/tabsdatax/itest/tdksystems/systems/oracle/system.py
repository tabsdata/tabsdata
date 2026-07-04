"""Oracle Database Free 23c. Domain == user/schema (FREEPDB1)."""
from __future__ import annotations

import os

from .._container import ContainerBase, DomainResult
from ...errors import DatagenError
from ...shell import docker_exec, docker_run, gen_password


ORACLE_SERVICE = "FREEPDB1"


class OracleSystem(ContainerBase):
    slug = "oracle"
    label = "Oracle Database Free"
    domain_desc = "a user/schema (Oracle treats them as one entity)"
    domain_flag = "database"
    supports_sql = True

    image = f"container-registry.oracle.com/database/free:{os.environ.get('ORACLE_IMAGE_VERSION', '23.6.0.0-lite')}"
    image_platform = "linux/amd64"
    internal_port = 1521
    system_user = "system"
    password_env_name = "ORACLE_PWD"
    extra_env: list[tuple[str, str]] = []
    extra_flags: list[str] = []

    def _docker_run(self, container: str, host_port: int, password: str) -> str:
        return docker_run(self._build_docker_args(container, host_port, password))

    def check_ready(self, container: str, password: str) -> bool:
        # checkDBStatus.sh only verifies the SMON process is alive — it returns
        # OK before setPassword.sh has propagated ORACLE_PWD to SYSTEM, so a
        # subsequent sqlplus connect can hit ORA-01017. Require a real auth
        # round-trip so readiness implies the admin password actually works.
        cp = docker_exec(
            container, "sqlplus", "-s", "-L",
            f"{self.system_user}/{password}@//localhost:1521/{ORACLE_SERVICE}",
            input="WHENEVER SQLERROR EXIT FAILURE;\nSELECT 1 FROM DUAL;\nEXIT;\n",
        )
        return cp.returncode == 0

    def _exec(self, container: str, password: str, sql: str) -> str:
        cp = docker_exec(
            container, "sqlplus", "-s", "-L",
            f"{self.system_user}/{password}@//localhost:1521/{ORACLE_SERVICE}",
            input=sql,
        )
        if cp.returncode != 0:
            raise DatagenError(f"sqlplus failed: {cp.stderr.strip() or cp.stdout.strip()}")
        return cp.stdout

    def do_create(self, container: str, password: str, name: str) -> DomainResult:
        pw = gen_password()
        self._exec(container, password, (
            "WHENEVER SQLERROR EXIT FAILURE;\n"
            "SET FEEDBACK OFF;\nSET HEADING OFF;\n"
            f'CREATE USER {name} IDENTIFIED BY "{pw}";\n'
            f"GRANT CONNECT, RESOURCE TO {name};\n"
            f"GRANT UNLIMITED TABLESPACE TO {name};\n"
            "EXIT;\n"
        ))
        port = os.environ.get("PORT", "1521")
        uri = f"oracle+oracledb://{name}:{pw}@localhost:{port}/?service_name={ORACLE_SERVICE}"
        return DomainResult(db=name, user=name, password=pw, sqlalchemy_uri=uri)

    def do_delete(self, container: str, password: str, name: str) -> None:
        self._exec(container, password, (
            "WHENEVER SQLERROR EXIT FAILURE;\n"
            "SET FEEDBACK OFF;\nSET HEADING OFF;\n"
            f"DROP USER {name} CASCADE;\nEXIT;\n"
        ))

    def do_list(self, container: str, password: str) -> list[str]:
        out = self._exec(container, password, (
            "SET PAGESIZE 0\nSET FEEDBACK OFF\nSET HEADING OFF\nSET TRIMSPOOL ON\n"
            "SELECT username FROM all_users WHERE oracle_maintained = 'N' "
            "ORDER BY username;\nEXIT;\n"
        ))
        return [line for line in (l.strip() for l in out.splitlines()) if line]
