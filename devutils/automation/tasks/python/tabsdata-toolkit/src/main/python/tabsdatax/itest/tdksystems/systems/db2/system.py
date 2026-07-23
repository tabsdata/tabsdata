#
# Copyright 2026 Tabsdata Inc.
#

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

from ...errors import DatagenError
from ...shell import docker_exec, docker_exec_check, docker_run
from .._container import AdminConn, ContainerBase, DomainResult

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
    max_entity_name_length = 128  # Db2 schema-name limit
    supports_sql = True

    image = (
        f"icr.io/db2_community/db2:{os.environ.get('DB2_IMAGE_VERSION', '11.5.9.0')}"
    )
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
            container,
            "su",
            "-",
            self.system_user,
            "-c",
            "db2gcf -s",
        )

    def _post_start(self, container: str, password: str) -> None:
        # Create the one shared database (idempotent) so domain creation only
        # has to add a schema. A single database also sidesteps the parallel-
        # database memory issues of the old database-per-domain model.
        if XP_DB not in self._list_databases(container):
            print(f">>> creating the shared Db2 database '{XP_DB}' …")
            self._exec_inst(
                container,
                f"set -e\ndb2 -v create database {XP_DB}\ndb2 -v terminate\n",
            )

    def _exec_inst(
        self, container: str, script: str, ignore_failure: bool = False
    ) -> str:
        cp = docker_exec(container, "bash", "-lc", script, user=self.system_user)
        if cp.returncode != 0 and not ignore_failure:
            raise DatagenError(
                f"db2 instance command failed: {cp.stderr.strip() or cp.stdout.strip()}"
            )
        return cp.stdout

    def _list_databases(self, container: str) -> list[str]:
        cp = docker_exec(
            container,
            "bash",
            "-lc",
            "db2 list database directory | awk '/Database name/ {print $NF}'",
            user=self.system_user,
        )
        if cp.returncode != 0:
            return []
        return [
            line for line in (entry.strip() for entry in cp.stdout.splitlines()) if line
        ]

    def _admin_url(self, admin: AdminConn) -> str:
        # Admin connects to the shared XP database over TCP as the instance
        # owner; domains are SCHEMAs inside it. The XP database itself is created
        # server-side in `_post_start` (a CLP command, not SQL) at system create.
        return (
            f"db2+ibm_db://{admin.user}:{admin.password}"
            f"@{admin.host}:{admin.port}/{XP_DB}"
        )

    def domain_result(self, admin: AdminConn, name: str, password: str) -> DomainResult:
        # Db2 domains reuse the instance (admin) credentials; the domain is a
        # schema, so the generated `password` is unused.
        schema = db2_schema_name(name)
        # currentSchema pins unqualified DDL / DML to this domain's schema.
        uri = (
            f"db2+ibm_db://{admin.user}:{admin.password}"
            f"@{admin.host}:{admin.port}/{XP_DB}?currentSchema={schema}"
        )
        return DomainResult(
            db=schema, user=admin.user, password=admin.password, sqlalchemy_uri=uri
        )

    def do_create(self, admin: AdminConn, result: DomainResult) -> None:
        self._run_admin_sql(admin, [f"CREATE SCHEMA {result.db}"])

    def do_delete(self, admin: AdminConn, name: str) -> None:
        # Drop the schema's objects, then the (now empty) schema. We avoid
        # SYSPROC.ADMIN_DROP_SCHEMA: on the community image it demands a
        # caller-managed error table and still fails (SQL0443N), and a bare CALL
        # with NULL error-table params is rejected (SQL0469N — its error-table
        # parameters are INOUT). Dropping objects then DROP SCHEMA … RESTRICT
        # needs no admin routine (the approach tabsdata-conn-db2 uses). The
        # shared XP database is left for the other domains.
        schema = db2_schema_name(name)
        if schema not in self.do_list(admin):
            return  # already gone — idempotent (avoids a noisy SQL0204N)
        views = self._query_admin_names(
            admin, f"SELECT viewname FROM syscat.views WHERE viewschema = '{schema}'"
        )
        if views:
            self._run_admin_sql(admin, [f"DROP VIEW {schema}.{v}" for v in views])
        # Tables, in FK-dependency order: retry until a pass drops nothing new
        # (a table referenced by an FK can only go after its dependants).
        remaining = self._query_admin_names(
            admin,
            f"SELECT tabname FROM syscat.tables "
            f"WHERE tabschema = '{schema}' AND type = 'T'",
        )
        while remaining:
            blocked: list[str] = []
            for tbl in remaining:
                try:
                    self._run_admin_sql(admin, [f"DROP TABLE {schema}.{tbl}"])
                except DatagenError:
                    blocked.append(tbl)  # referenced by an FK — retry next pass
            if len(blocked) == len(remaining):
                break  # no progress — let DROP SCHEMA surface what's left
            remaining = blocked
        self._run_admin_sql(admin, [f"DROP SCHEMA {schema} RESTRICT"])

    def do_list(self, admin: AdminConn) -> list[str]:
        return self._query_admin_names(
            admin,
            (
                "SELECT TRIM(schemaname) FROM syscat.schemata "
                "WHERE schemaname NOT LIKE 'SYS%' "
                "AND schemaname NOT IN ('NULLID','SQLJ','DB2INST1','DB2GSE','DB2QP') "
                "ORDER BY schemaname"
            ),
        )
