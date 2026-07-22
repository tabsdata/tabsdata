#
# Copyright 2026 Tabsdata Inc.
#

"""MySQL configured for change-data-capture.

A thin variant of the plain `mysql` system: same image and domain model, but
the container runs with the full binlog configuration a CDC consumer needs
(ROW format, FULL row image + metadata, GTID on) and each domain user also
gets the `RELOAD` global privilege (for `FLUSH TABLES WITH READ LOCK` during a
snapshot). Capture is server-wide, so `configure_cdc` has nothing per-table to
do. Reuses the base model DDL (`mysql.ddl`) via `ddl_slug`.
"""

from __future__ import annotations

from ..mysql.system import MysqlSystem


class MysqlCdcSystem(MysqlSystem):
    slug = "mysqlcdc"
    label = "MySQL (CDC)"

    # Full binlog config for downstream CDC (superset of the plain system's):
    # ROW format with FULL before-image + metadata, and GTID enabled.
    cmd_args = [
        "--server-id=1",
        "--log-bin=mysql-bin",
        "--binlog-format=ROW",
        "--binlog-row-image=FULL",
        "--binlog-row-metadata=FULL",
        "--gtid-mode=ON",
        "--enforce-gtid-consistency=ON",
        "--binlog-expire-logs-seconds=259200",
    ]

    @property
    def ddl_slug(self) -> str:
        return "mysql"

    def do_create(self, admin, result) -> None:
        super().do_create(admin, result)
        # RELOAD is a global privilege (not covered by the db-scoped ALL grant)
        # that a CDC snapshot needs for FLUSH TABLES WITH READ LOCK.
        self._run_admin_sql(
            admin,
            [
                f"GRANT RELOAD ON *.* TO '{result.user}'@'%'",
                "FLUSH PRIVILEGES",
            ],
        )

    def configure_cdc(self, ctx, model: str) -> None:
        # MySQL binlog captures the whole server — no per-table enablement.
        print(">>> CDC: MySQL binlog captures all tables (no per-table setup)")
