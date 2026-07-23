#
# Copyright 2026 Tabsdata Inc.
#

"""MySQL configured for change-data-capture.

A thin variant of the plain ``mysql`` system — same image, domain model and DDL
(reused via ``ddl_from_slug``) — but the container runs the full binlog config a
CDC consumer needs (ROW format, FULL before-image + metadata, GTID on) and each
domain user also gets ``RELOAD`` (for ``FLUSH TABLES WITH READ LOCK`` during a
snapshot). Capture is server-wide, so there is no per-table setup.
"""

from __future__ import annotations

from tabsdatax.itest.tdksystems2.systems.mysql.backend import Mysql


class Mysqlcdc(Mysql):
    slug = "mysqlcdc"
    label = "MySQL (CDC)"
    ddl_from_slug = "mysql"

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

    def create_domain_sql(self, db: str, user: str, password: str) -> list[str]:
        # RELOAD is a global privilege a CDC snapshot needs for FLUSH TABLES.
        return super().create_domain_sql(db, user, password) + [
            f"GRANT RELOAD ON *.* TO '{user}'@'%'",
            "FLUSH PRIVILEGES",
        ]
