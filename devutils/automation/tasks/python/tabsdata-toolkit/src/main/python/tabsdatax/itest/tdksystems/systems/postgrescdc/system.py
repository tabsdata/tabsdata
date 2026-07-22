#
# Copyright 2026 Tabsdata Inc.
#

"""PostgreSQL configured for change-data-capture (logical decoding via wal2json).

A variant of the plain `postgres` system: same image and single-DB-per-domain
model, but the server runs with `wal_level=logical` (plus generous slot / sender
limits), the `wal2json` output plugin is installed at start (the base image
doesn't ship it), and each domain's owner role gets the `REPLICATION`
attribute. `configure_cdc` sets `REPLICA IDENTITY FULL` on every model table so
UPDATE / DELETE change records carry full before-images. The connector under
test creates its own replication slot at run time, so none is pre-created here.
Reuses the base model DDL (`postgres.ddl`) via `ddl_slug`.
"""

from __future__ import annotations

import os

from ...errors import DatagenError
from ...shell import docker_exec
from ..postgres.system import PostgresSystem


class PostgresCdcSystem(PostgresSystem):
    slug = "postgrescdc"
    label = "PostgreSQL (CDC)"

    # Logical decoding, matching the connector's CDC test config.
    cmd_args = [
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

    @property
    def ddl_slug(self) -> str:
        return "postgres"

    @staticmethod
    def _pg_major() -> str:
        """The Postgres major version, from the image tag (default 17) — the
        `postgresql-<major>-wal2json` package name needs it."""
        tag = os.environ.get("POSTGRES_IMAGE_VERSION", "17")
        return tag.split(".", 1)[0].split("-", 1)[0] or "17"

    def _post_start(self, container: str, password: str) -> None:
        super()._post_start(container, password)
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
            raise DatagenError(
                f"could not install {pkg} (needed for Postgres CDC): "
                f"{cp.stderr.strip() or cp.stdout.strip()}"
            )

    def do_create(self, admin, result) -> None:
        super().do_create(admin, result)
        # The domain owner needs the REPLICATION attribute to open a
        # logical-replication connection / create a slot.
        self._run_admin_sql(admin, [f'ALTER ROLE "{result.user}" WITH REPLICATION'])

    def configure_cdc(self, ctx, model: str) -> None:
        from ...sql_runner import run_sql_text

        tables = self._reflect_table_names(ctx)
        if not tables:
            print(">>> CDC: no tables to configure")
            return
        print(
            f">>> CDC: setting REPLICA IDENTITY FULL on {len(tables)} table(s) "
            f"for full before-images"
        )
        sql = "\n".join(f'ALTER TABLE "{t}" REPLICA IDENTITY FULL;' for t in tables)
        run_sql_text(self._require_uri(ctx), sql)
