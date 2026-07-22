#
# Copyright 2026 Tabsdata Inc.
#

"""MySQL domain login: the per-domain user is the database name, trimmed to
MySQL's 32-char login limit (with a warning when trimming risks a collision).
The database itself keeps the full name (MySQL's 64-char identifier limit)."""
from __future__ import annotations

from tabsdatax.itest.tdksystems.systems._container import AdminConn
from tabsdatax.itest.tdksystems.systems.mysql.system import MysqlSystem


def _admin() -> AdminConn:
    return AdminConn(host="localhost", port="3306", user="root", password="rootpw")


def test_mysql_login_is_the_database_name():
    sysobj = MysqlSystem()
    # A name within the 32-char limit: login == database name.
    res = sysobj.domain_result(_admin(), "finance_in", "pw")
    assert res.db == "finance_in"
    assert res.user == "finance_in"
    assert res.user in res.sqlalchemy_uri


def test_mysql_login_trimmed_to_32_with_warning(capsys):
    sysobj = MysqlSystem()
    long_name = "d" * 40                    # > 32 chars
    res = sysobj.domain_result(_admin(), long_name, "pw")
    assert res.db == long_name              # database keeps the full name (64 limit)
    assert res.user == long_name[:32]       # login trimmed to 32
    assert len(res.user) == 32
    err = capsys.readouterr().err
    assert "WARNING" in err and "trimming" in err


def test_mysql_no_warning_within_limit(capsys):
    sysobj = MysqlSystem()
    sysobj.domain_result(_admin(), "short_db", "pw")
    assert capsys.readouterr().err == ""
