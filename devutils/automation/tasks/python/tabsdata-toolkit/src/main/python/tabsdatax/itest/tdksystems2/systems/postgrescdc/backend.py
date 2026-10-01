#
# Copyright 2026 Tabsdata Inc.
#

"""PostgreSQL configured for change-data-capture (logical decoding via wal2json).

A variant of the plain ``postgres`` system — same image, domain model and DDL
(reused via ``ddl_from_slug``) — but the server runs with ``wal_level=logical``
(plus generous slot/sender limits), the ``wal2json`` output plugin is installed
at start (the base image doesn't ship it), each domain owner gets the
``REPLICATION`` attribute, and every model table is set ``REPLICA IDENTITY FULL``
so UPDATE/DELETE change records carry full before-images.
"""

from __future__ import annotations

from tabsdatax.itest.tdksystems2 import datagen
from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.shell import docker_exec
from tabsdatax.itest.tdksystems2.sql_runner import run_sql_text
from tabsdatax.itest.tdksystems2.systems._common import container_info
from tabsdatax.itest.tdksystems2.systems.postgres.backend import Postgres


class Postgrescdc(Postgres):
    slug = "postgrescdc"
    label = "PostgreSQL (CDC)"
    ddl_from_slug = "postgres"

    cmd_args = [
        *Postgres.cmd_args,
        "-c",
        "wal_level=logical",
        "-c",
        "max_replication_slots=1024",
        "-c",
        "max_wal_senders=1024",
        "-c",
        "wal_keep_size=1024",
        "-c",
        "max_logical_replication_workers=4",
    ]

    @staticmethod
    def _pg_major() -> str:
        return container_info.POSTGRES.version.split(".", 1)[0].split("-", 1)[0] or "17"

    def post_provision(self, container: str, password: str) -> None:
        super().post_provision(container, password)
        pkg = f"postgresql-{self._pg_major()}-wal2json"
        print(f">>> installing {pkg} (wal2json output plugin) for logical decoding …")
        cp = docker_exec(
            container,
            "bash",
            "-lc",
            f"apt-get update && apt-get install -y {pkg}",
            user="root",
        )
        if cp.returncode != 0:
            raise TdxError(
                f"could not install {pkg} (needed for Postgres CDC): "
                f"{cp.stderr.strip() or cp.stdout.strip()}"
            )
        self._allow_wal2json(container)

    def provision_domain(
        self,
        sys_meta: dict[str, str],
        entity_name: str,
        domain_meta: dict[str, str],
        *,
        force: bool = False,
    ) -> None:
        """A system created before :meth:`_allow_wal2json` existed is allowed
        here, so every domain of it can open a wal2json slot."""
        container = sys_meta.get("CONTAINER", "")
        if container:
            self._allow_wal2json(container)
        super().provision_domain(sys_meta, entity_name, domain_meta, force=force)

    def _psql(self, container: str, sql: str) -> str:
        cp = docker_exec(container, "psql", "-U", self.root_user, "-qAtX", "-c", sql)
        if cp.returncode != 0:
            raise TdxError(
                f"psql failed in {container}: {cp.stderr.strip() or cp.stdout.strip()}"
            )
        return cp.stdout.strip()

    def _allow_wal2json(self, container: str) -> None:
        """Let replication slots use wal2json.

        Since the August 2026 minor releases (18.6, 17.11, 16.15, 15.19, 14.24)
        a slot may only use the output plugins ``output_plugin_libraries``
        lists, by default only PostgreSQL's own (``pgoutput``,
        ``test_decoding``). An older server has no such setting and allows any
        installed plugin, so it is left alone."""
        present, value = self._psql(
            container,
            "select current_setting('output_plugin_libraries', true) is not null, "
            "coalesce(current_setting('output_plugin_libraries', true), '')",
        ).split("|", 1)
        if present != "t":
            return
        allowed = [name.strip().strip('"') for name in value.split(",") if name.strip()]
        if "wal2json" in allowed:
            return
        print(">>> allowing wal2json as a logical decoding output plugin …")
        # A list setting takes one quoted value per element: a single quoted
        # string would be one library named after the whole list.
        elements = ", ".join(f"'{name}'" for name in [*allowed, "wal2json"])
        self._psql(container, f"alter system set output_plugin_libraries = {elements}")
        self._psql(container, "select pg_reload_conf()")
        shown = self._psql(container, "show output_plugin_libraries")
        if "wal2json" not in [name.strip().strip('"') for name in shown.split(",")]:
            raise TdxError(
                f"wal2json is still not allowed as an output plugin in {container} "
                f"(output_plugin_libraries = {shown})"
            )

    def create_domain_sql(self, db: str, user: str, password: str) -> list[str]:
        # The owner role needs REPLICATION to open a logical-replication connection.
        return super().create_domain_sql(db, user, password) + [
            f'ALTER ROLE "{user}" WITH REPLICATION'
        ]

    def after_install_schema(self, meta: dict[str, str], model: str) -> None:
        tables = datagen.entities_of(model)
        if not tables:
            return
        print(f">>> CDC: REPLICA IDENTITY FULL on {len(tables)} table(s)")
        run_sql_text(
            meta["SQLALCHEMY_URI"],
            "\n".join(f'ALTER TABLE "{t}" REPLICA IDENTITY FULL;' for t in tables),
        )
