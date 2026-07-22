#
# Copyright 2026 Tabsdata Inc.
#

"""mssqlserver domains are DATABASES, each with a dedicated login that owns it.
Destroy drops the database (after forcing it single-user) and the login. A
destroy of a database that is already gone (container restarted with lost data,
or the system recreated) must be a clean no-op — ALTER DATABASE on a missing db
raises Msg 911 and there is no IF EXISTS here."""
from __future__ import annotations

from tabsdatax.itest.tdksystems.systems._container import AdminConn
from tabsdatax.itest.tdksystems.systems.mssqlserver.system import MssqlSystem


def _admin() -> AdminConn:
    return AdminConn(host="localhost", port="1433", user="sa", password="pw")


def test_do_delete_forces_single_user_then_drops_db_and_login(monkeypatch):
    sysobj = MssqlSystem()
    statements: list[str] = []
    monkeypatch.setattr(sysobj, "_run_admin_sql",
                        lambda admin, stmts: statements.extend(stmts))
    # the database exists (do_list reports it).
    monkeypatch.setattr(sysobj, "_query_admin_names", lambda admin, sql: ["D_IN"])
    sysobj.do_delete(_admin(), "D_IN")
    assert statements == [
        "ALTER DATABASE [D_IN] SET SINGLE_USER WITH ROLLBACK IMMEDIATE",
        "DROP DATABASE [D_IN]",
        "DROP LOGIN [D_IN]",
    ]


def test_do_delete_is_idempotent_when_database_absent(monkeypatch):
    # destroy of an absent database (e.g. after the container was rebuilt) is a
    # clean no-op — no ALTER/DROP is attempted (which would raise Msg 911).
    sysobj = MssqlSystem()
    statements: list[str] = []
    monkeypatch.setattr(sysobj, "_run_admin_sql",
                        lambda admin, stmts: statements.extend(stmts))
    monkeypatch.setattr(sysobj, "_query_admin_names", lambda admin, sql: [])
    sysobj.do_delete(_admin(), "gone")
    assert statements == []
