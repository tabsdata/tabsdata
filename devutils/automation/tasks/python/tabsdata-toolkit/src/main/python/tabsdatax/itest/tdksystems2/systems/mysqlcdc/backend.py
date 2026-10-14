#
# Copyright 2026 Tabsdata Inc.
#

"""MySQL configured for change-data-capture.

A thin variant of the plain ``mysql`` system — same image, domain model and DDL
(reused via ``ddl_from_slug``) — but the container runs the full binlog config a
CDC consumer needs (ROW format, FULL before-image + metadata, GTID on) and each
domain user also gets ``RELOAD`` (for ``FLUSH TABLES WITH READ LOCK`` during a
snapshot). Each domain also gets a second user, ``NO_LOCK_USER``, with what a CDC
stream needs but not ``RELOAD``: the server refuses it the global read lock, as
managed services such as Amazon RDS refuse it to every user, so an initial load
through it takes the path without the lock. Capture is server-wide, so there is
no per-table setup.
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

    def _no_lock_login(self, name: str) -> str:
        return self._login(f"{name}_nolock")

    def create_domain_sql(self, db: str, user: str, password: str) -> list[str]:
        # RELOAD is a global privilege a CDC snapshot needs for FLUSH TABLES.
        no_lock = self._no_lock_login(db)
        return super().create_domain_sql(db, user, password) + [
            f"GRANT RELOAD ON *.* TO '{user}'@'%'",
            f"CREATE USER '{no_lock}'@'%' IDENTIFIED BY '{password}'",
            f"GRANT SELECT ON `{db}`.* TO '{no_lock}'@'%'",
            f"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{no_lock}'@'%'",
            "FLUSH PRIVILEGES",
        ]

    def delete_domain_sql(self, name: str) -> list[str]:
        return [
            f"DROP USER IF EXISTS '{self._no_lock_login(name)}'@'%'",
            *super().delete_domain_sql(name),
        ]

    def domain_meta(
        self, sys_meta: dict[str, str], entity_name: str, password: str
    ) -> dict[str, str]:
        return {
            **super().domain_meta(sys_meta, entity_name, password),
            "NO_LOCK_USER": self._no_lock_login(entity_name),
        }
