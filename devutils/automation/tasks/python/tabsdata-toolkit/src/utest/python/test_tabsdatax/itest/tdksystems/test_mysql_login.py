"""MySQL domain login: the per-domain user is the database name, trimmed to
MySQL's 32-char login limit (with a warning when trimming risks a collision).
The database itself keeps the full name (MySQL's 64-char identifier limit)."""
from __future__ import annotations

from tabsdatax.itest.tdksystems.systems.mysql.system import MysqlSystem


def test_mysql_login_is_the_database_name(monkeypatch):
    sysobj = MysqlSystem()
    monkeypatch.setattr(sysobj, "_exec_sql", lambda c, p, sql: "")
    monkeypatch.setenv("PORT", "3306")
    # A name within the 32-char limit: login == database name.
    res = sysobj.do_create("mysql", "rootpw", "finance_in")
    assert res.db == "finance_in"
    assert res.user == "finance_in"
    assert res.user in res.sqlalchemy_uri


def test_mysql_login_trimmed_to_32_with_warning(monkeypatch, capsys):
    sysobj = MysqlSystem()
    monkeypatch.setattr(sysobj, "_exec_sql", lambda c, p, sql: "")
    monkeypatch.setenv("PORT", "3306")
    long_name = "d" * 40                    # > 32 chars
    res = sysobj.do_create("mysql", "rootpw", long_name)
    assert res.db == long_name              # database keeps the full name (64 limit)
    assert res.user == long_name[:32]       # login trimmed to 32
    assert len(res.user) == 32
    err = capsys.readouterr().err
    assert "WARNING" in err and "trimming" in err


def test_mysql_no_warning_within_limit(monkeypatch, capsys):
    sysobj = MysqlSystem()
    monkeypatch.setattr(sysobj, "_exec_sql", lambda c, p, sql: "")
    monkeypatch.setenv("PORT", "3306")
    sysobj.do_create("mysql", "rootpw", "short_db")
    assert capsys.readouterr().err == ""
