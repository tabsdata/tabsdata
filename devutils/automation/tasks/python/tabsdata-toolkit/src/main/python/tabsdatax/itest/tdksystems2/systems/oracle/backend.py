#
# Copyright 2026 Tabsdata Inc.
#

"""Oracle Database Free 23c backend — a docker-run container.

A domain is a user/schema (Oracle treats them as one) inside the ``FREEPDB1``
pluggable database. Admin runs as ``SYSTEM`` over oracledb (thin mode); readiness
requires a real auth round-trip (the image reports the SMON process alive before
the SYSTEM password is usable). Only the Oracle dialect hooks live here.
"""

from __future__ import annotations

from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.shell import docker_exec
from tabsdatax.itest.tdksystems2.systems._common import container_info
from tabsdatax.itest.tdksystems2.systems._common.sql import Admin, SqlContainerBackend

_SERVICE = "FREEPDB1"


class Oracle(SqlContainerBackend):
    slug = "oracle"
    label = "Oracle Database Free"
    weight = 2  # slow to boot — schedule ahead of the light systems
    domain_word = "schema"
    entity_aliases = ("DATABASE",)  # fixtures also expose the schema as `database`
    max_entity_name_length = 128
    login_maxlen = 128

    image = container_info.ORACLE.ref
    image_platform = container_info.ORACLE.platform
    internal_port = 1521
    root_user = "system"
    driver = "oracle+oracledb"
    insert_batch = 1  # Oracle has no multi-row VALUES

    def session_setup(self) -> list[str]:
        # So the generator's ISO date/timestamp literals parse regardless of the
        # image's NLS defaults.
        return [
            "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'",
            "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'",
        ]

    def drop_all_tables(self, uri: str) -> None:
        from sqlalchemy import create_engine

        engine = create_engine(uri)
        try:
            with engine.begin() as conn:
                names = [
                    r[0]
                    for r in conn.exec_driver_sql(
                        "SELECT table_name FROM user_tables"
                    ).fetchall()
                ]
                if names:
                    print(f">>> dropping {len(names)} existing table(s) …")
                for name in names:
                    conn.exec_driver_sql(
                        f'DROP TABLE "{name}" CASCADE CONSTRAINTS PURGE'
                    )
        finally:
            engine.dispose()

    def sqlalchemy_url(
        self, host: str, port: str, db: str = "", user: str = "", password: str = ""
    ) -> str:
        creds = f"{user}:{password}@" if user else ""
        return f"{self.driver}://{creds}{host}:{port}/?service_name={_SERVICE}"

    def env_vars(self, password: str) -> dict[str, str]:
        return {"ORACLE_PWD": password}

    def check_ready(self, container: str, password: str) -> bool:
        cp = docker_exec(
            container,
            "sqlplus",
            "-s",
            "-L",
            f"{self.root_user}/{password}@//localhost:1521/{_SERVICE}",
            input="WHENEVER SQLERROR EXIT FAILURE;\nSELECT 1 FROM DUAL;\nEXIT;\n",
        )
        return cp.returncode == 0

    def create_domain_sql(self, db: str, user: str, password: str) -> list[str]:
        return [
            f'CREATE USER {user} IDENTIFIED BY "{password}"',
            f"GRANT CONNECT, RESOURCE TO {user}",
            f"GRANT UNLIMITED TABLESPACE TO {user}",
        ]

    def delete_domain_sql(self, name: str) -> list[str]:
        return [f"DROP USER {name} CASCADE"]

    def _do_delete(self, admin: Admin, name: str) -> None:
        if not any(u.lower() == name.lower() for u in self._do_list(admin)):
            return  # already gone — idempotent (avoids ORA-01918)
        try:  # best-effort: free the schema's sessions so DROP USER can proceed
            self._run_admin(admin, [self._kill_sessions_block(name)])
        except TdxError:
            pass
        super()._do_delete(admin, name)

    @staticmethod
    def _kill_sessions_block(name: str) -> str:
        return (
            "BEGIN "
            "FOR s IN (SELECT sid, serial# FROM v$session "
            f"WHERE username = UPPER('{name}')) LOOP "
            "BEGIN EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' "
            "|| s.sid || ',' || s.serial# || ''' IMMEDIATE'; "
            "EXCEPTION WHEN OTHERS THEN NULL; END; "
            "END LOOP; END;"
        )

    def list_schemas_sql(self) -> str:
        return (
            "SELECT username FROM all_users WHERE oracle_maintained = 'N' "
            "ORDER BY username"
        )
