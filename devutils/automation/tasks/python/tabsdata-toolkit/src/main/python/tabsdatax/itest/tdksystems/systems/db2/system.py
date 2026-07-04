"""IBM Db2 Community 11.5. Domain == a SCHEMA inside one shared database.

Db2 database names are capped at 8 characters — far too short for the prefixed
domain names (D_<timestamp>_<owner>_<logical>). So instead of a database per
domain we keep ONE database (``XP``) and create a SCHEMA per domain (schema
names allow 128 chars). This also matches how the other SQL warehouses
(mssqlserver / snowflake / databricks) model domains, and avoids the
multiple-databases-in-parallel memory pressure (SQL1043C/-952) the per-database
model hit under the community image's default container memory.

We connect as the instance owner (db2inst1) rather than minting a per-domain OS
user: Db2's OS-level authentication of dynamically-created users is unreliable
in the community image (SQL30082N rc=24), whereas the instance owner
authenticates fine and has full authority over the database it created. Each
domain's connection pins ``currentSchema`` so unqualified DDL / DML lands in the
right schema.
"""
from __future__ import annotations

import os

from .._container import ContainerBase, DomainResult
from ...errors import DatagenError
from ...shell import docker_exec, docker_exec_check, docker_run

# The single database that holds every domain (as a schema). Must be <= 8
# chars (Db2's database-name limit).
XP_DB = "XP"


def db2_schema_name(name: str) -> str:
    """Db2 folds unquoted identifiers to upper case; normalise the prefixed
    domain name to the schema name Db2 actually stores."""
    return name.upper()


class Db2System(ContainerBase):
    slug = "db2"
    label = "IBM Db2 Community"
    domain_desc = "a schema inside the shared 'XP' database"
    domain_flag = "schema"
    supports_sql = True

    image = f"icr.io/db2_community/db2:{os.environ.get('DB2_IMAGE_VERSION', '11.5.9.0')}"
    image_platform = "linux/amd64"
    internal_port = 50000
    system_user = "db2inst1"
    password_env_name = "DB2INST1_PASSWORD"
    # Don't preload a SAMPLE database — we create our own 'XP' database in
    # _post_start. LICENSE=accept is required by the community image.
    extra_env: list[tuple[str, str]] = [("LICENSE", "accept")]
    extra_flags: list[str] = ["--privileged"]

    def _docker_run(self, container: str, host_port: int, password: str) -> str:
        return docker_run(self._build_docker_args(container, host_port, password))

    def check_ready(self, container: str, password: str) -> bool:
        # Instance-level probe — succeeds once db2start is complete, even with
        # no databases created yet. db2gcf -s exits 0 when the instance is
        # "Available" (engine listening), 1 otherwise.
        return docker_exec_check(
            container, "su", "-", self.system_user, "-c", "db2gcf -s",
        )

    def _post_start(self, container: str, password: str) -> None:
        # Create the one shared database (idempotent) so domain creation only
        # has to add a schema. A single database also sidesteps the parallel-
        # database memory issues of the old database-per-domain model.
        if XP_DB not in self._list_databases(container):
            print(f">>> creating the shared Db2 database '{XP_DB}' …")
            self._exec_inst(
                container,
                f"set -e\n"
                f"db2 -v create database {XP_DB}\n"
                f"db2 -v terminate\n",
            )

    def _exec_inst(self, container: str, script: str, ignore_failure: bool = False) -> str:
        cp = docker_exec(container, "bash", "-lc", script, user=self.system_user)
        if cp.returncode != 0 and not ignore_failure:
            raise DatagenError(f"db2 instance command failed: {cp.stderr.strip() or cp.stdout.strip()}")
        return cp.stdout

    def _list_databases(self, container: str) -> list[str]:
        cp = docker_exec(
            container, "bash", "-lc",
            "db2 list database directory | awk '/Database name/ {print $NF}'",
            user=self.system_user,
        )
        if cp.returncode != 0:
            return []
        return [line for line in (l.strip() for l in cp.stdout.splitlines()) if line]

    def do_create(self, container: str, password: str, name: str) -> DomainResult:
        # Belt-and-braces: make sure the shared database exists (it normally
        # does, from _post_start), then create the domain's schema in it.
        self._post_start(container, password)
        schema = db2_schema_name(name)
        self._exec_inst(
            container,
            f"set -e\n"
            f"db2 -v connect to {XP_DB}\n"
            f'db2 -v "CREATE SCHEMA {schema}"\n'
            f"db2 -v connect reset\n"
            f"db2 -v terminate\n",
        )
        port = os.environ.get("PORT", "50000")
        # currentSchema pins unqualified DDL / DML to this domain's schema.
        uri = (f"db2+ibm_db://{self.system_user}:{password}@localhost:{port}/"
               f"{XP_DB}?currentSchema={schema}")
        return DomainResult(db=schema, user=self.system_user, password=password,
                            sqlalchemy_uri=uri)

    def do_delete(self, container: str, password: str, name: str) -> None:
        # ADMIN_DROP_SCHEMA drops the schema and all its objects (plain DROP
        # SCHEMA … RESTRICT refuses a non-empty schema). Best-effort on
        # teardown; the database 'XP' itself is left for the other domains.
        schema = db2_schema_name(name)
        self._exec_inst(
            container,
            f"db2 -v connect to {XP_DB}\n"
            f"db2 -v \"CALL SYSPROC.ADMIN_DROP_SCHEMA('{schema}', NULL, NULL, NULL)\"\n"
            f"db2 -v connect reset\n"
            f"db2 -v terminate\n",
            ignore_failure=True,
        )

    def do_list(self, container: str, password: str) -> list[str]:
        cp = docker_exec(
            container, "bash", "-lc",
            f"db2 connect to {XP_DB} >/dev/null 2>&1 && "
            f"db2 -x \"SELECT TRIM(schemaname) FROM syscat.schemata "
            f"WHERE schemaname NOT LIKE 'SYS%' "
            f"AND schemaname NOT IN ('NULLID','SQLJ','DB2INST1','DB2GSE','DB2QP') "
            f"ORDER BY schemaname\"",
            user=self.system_user,
        )
        if cp.returncode != 0:
            return []
        return [line for line in (l.strip() for l in cp.stdout.splitlines()) if line]
