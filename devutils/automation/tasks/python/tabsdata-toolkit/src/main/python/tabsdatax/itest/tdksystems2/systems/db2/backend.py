#
# Copyright 2026 Tabsdata Inc.
#

"""IBM Db2 Community backend — a docker-run container, schema-per-domain.

Db2 database names are capped at 8 chars, far too short for the prefixed domain
names, so (as v1 does, matching the other warehouses) there is ONE shared
database ``XP`` and each domain is a SCHEMA inside it. Admin connects to ``XP``
as the instance owner ``db2inst1``; a domain's connection pins ``currentSchema``
so unqualified DDL/DML lands in the right schema. The image needs ``--privileged``
and ``LICENSE=accept``, and the ``XP`` database is created after start with a CLP
command (not SQL).
"""

from __future__ import annotations

import re

from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.shell import docker_exec, docker_exec_check
from tabsdatax.itest.tdksystems2.systems._common import container_info
from tabsdatax.itest.tdksystems2.systems._common.sql import Admin, SqlContainerBackend

_XP_DB = "XP"  # the one shared database (<= 8 chars, Db2's DB-name limit)


class Db2(SqlContainerBackend):
    slug = "db2"
    label = "IBM Db2 Community"
    weight = 3  # slow to boot (archive log + backup for CDC) — schedule first
    domain_word = "schema"
    max_entity_name_length = 128  # Db2 schema-name limit

    image = container_info.DB2.ref
    image_platform = container_info.DB2.platform
    internal_port = 50000
    root_user = "db2inst1"
    driver = "db2+ibm_db"
    admin_db = _XP_DB
    ready_timeout = 600
    extra_run_flags = ["--privileged"]
    system_schemas = frozenset({"nullid", "sqlj", "db2inst1", "db2gse", "db2qp"})

    @staticmethod
    def _schema(name: str) -> str:
        # Db2 folds unquoted identifiers to upper case.
        return name.upper()

    def env_vars(self, password: str) -> dict[str, str]:
        return {"LICENSE": "accept", "DB2INST1_PASSWORD": password}

    def check_ready(self, container: str, password: str) -> bool:
        # Instance-level probe: db2gcf -s exits 0 once the engine is listening.
        return docker_exec_check(
            container, "su", "-", self.root_user, "-c", "db2gcf -s"
        )

    def post_provision(self, container: str, password: str) -> None:
        if _XP_DB not in self._list_databases(container):
            print(f">>> creating the shared Db2 database '{_XP_DB}' …")
            cp = docker_exec(
                container,
                "bash",
                "-lc",
                f"set -e\ndb2 -v create database {_XP_DB}\ndb2 -v terminate\n",
                user=self.root_user,
            )
            if cp.returncode != 0:
                raise TdxError(
                    f"db2 create database {_XP_DB} failed: "
                    f"{cp.stderr.strip() or cp.stdout.strip()}"
                )

    def _list_databases(self, container: str) -> list[str]:
        cp = docker_exec(
            container,
            "bash",
            "-lc",
            "db2 list database directory | awk '/Database name/ {print $NF}'",
            user=self.root_user,
        )
        if cp.returncode != 0:
            return []
        return [ln.strip() for ln in cp.stdout.splitlines() if ln.strip()]

    def domain_meta(
        self, sys_meta: dict[str, str], entity_name: str, password: str
    ) -> dict[str, str]:
        # A domain reuses the instance (admin) credentials; the domain is a
        # schema, so the generated `password` is unused. currentSchema pins DDL/DML.
        admin = self._admin(sys_meta)
        schema = self._schema(entity_name)
        base = f"{admin.host}:{admin.port}/{_XP_DB}?currentSchema={schema}"
        return {
            "HOST": admin.host,
            "PORT": admin.port,
            "USER": admin.user,
            "PASSWORD": admin.password,
            "DB": schema,
            "SQLALCHEMY_URI": f"{self.driver}://{admin.user}:{admin.password}@{base}",
            "URI": f"{self.driver}://{base}",
            "IMAGE": sys_meta.get("IMAGE", ""),
            "SYS_CONTAINER": sys_meta.get("CONTAINER", ""),
            "SYS_CONTAINER_ID": sys_meta.get("CONTAINER_ID", ""),
            "SYS_ADMIN_USER": admin.user,
            "SYS_ADMIN_PASSWORD": admin.password,
        }

    def create_domain_sql(self, db: str, user: str, password: str) -> list[str]:
        return [f"CREATE SCHEMA {db}"]

    def _do_delete(self, admin: Admin, name: str) -> None:
        # Drop the schema's views + tables (FK order), then the empty schema.
        schema = self._schema(name)
        if schema not in self._do_list(admin):
            return  # already gone — idempotent (avoids SQL0204N)
        views = self._query_admin(
            admin, f"SELECT viewname FROM syscat.views WHERE viewschema = '{schema}'"
        )
        if views:
            self._run_admin(admin, [f"DROP VIEW {schema}.{v}" for v in views])
        remaining = self._query_admin(
            admin,
            f"SELECT tabname FROM syscat.tables "
            f"WHERE tabschema = '{schema}' AND type = 'T'",
        )
        while remaining:
            blocked: list[str] = []
            for tbl in remaining:
                try:
                    self._run_admin(admin, [f"DROP TABLE {schema}.{tbl}"])
                except TdxError:
                    blocked.append(tbl)  # referenced by an FK — retry next pass
            if len(blocked) == len(remaining):
                break
            remaining = blocked
        self._run_admin(admin, [f"DROP SCHEMA {schema} RESTRICT"])

    def list_schemas_sql(self) -> str:
        return (
            "SELECT TRIM(schemaname) FROM syscat.schemata "
            "WHERE schemaname NOT LIKE 'SYS%' "
            "AND schemaname NOT IN ('NULLID','SQLJ','DB2INST1','DB2GSE','DB2QP') "
            "ORDER BY schemaname"
        )

    def drop_all_tables(self, uri: str) -> None:
        # Base reflection targets the wrong schema for a currentSchema domain;
        # drop this schema's views + tables (FK order) explicitly, keeping the
        # schema itself (create-data reloads into it). ibm_db_sa has no AUTOCOMMIT
        # isolation, so each drop commits on its own (rollback + retry on FK block).
        from sqlalchemy import create_engine

        m = re.search(r"currentSchema=([^&]+)", uri)
        if not m:
            return
        schema = m.group(1)

        def names(conn, query: str) -> list[str]:
            rows = [r[0] for r in conn.exec_driver_sql(query).fetchall()]
            conn.rollback()
            return rows

        def drop(conn, stmt: str) -> bool:
            try:
                conn.exec_driver_sql(stmt)
                conn.commit()
                return True
            except Exception:  # noqa: BLE001 - FK-blocked / absent; caller retries
                conn.rollback()
                return False

        engine = create_engine(uri)
        try:
            with engine.connect() as conn:
                for view in names(
                    conn,
                    f"SELECT viewname FROM syscat.views WHERE viewschema = '{schema}'",
                ):
                    drop(conn, f'DROP VIEW "{schema}"."{view}"')
                remaining = names(
                    conn,
                    f"SELECT tabname FROM syscat.tables "
                    f"WHERE tabschema = '{schema}' AND type = 'T'",
                )
                if remaining:
                    print(f">>> dropping {len(remaining)} existing table(s) …")
                while remaining:
                    blocked = [
                        t
                        for t in remaining
                        if not drop(conn, f'DROP TABLE "{schema}"."{t}"')
                    ]
                    if len(blocked) == len(remaining):
                        break
                    remaining = blocked
        finally:
            engine.dispose()
