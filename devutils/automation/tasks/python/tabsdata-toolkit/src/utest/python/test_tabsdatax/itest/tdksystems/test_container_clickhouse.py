"""ClickHouse domains are databases with a dedicated owner user. Beyond owning
its database, that user needs the global privileges the s3/gcs staged INSERT
relies on (the ``s3()`` table function reads into a temporary table)."""
from __future__ import annotations

from tabsdatax.itest.tdksystems.systems._container import AdminConn
from tabsdatax.itest.tdksystems.systems.clickhouse.system import ClickhouseSystem


def _admin() -> AdminConn:
    return AdminConn(host="localhost", port="18123", user="default", password="adminpw")


def test_do_create_grants_s3_and_temp_table_globally(monkeypatch):
    sysobj = ClickhouseSystem()
    statements: list[str] = []
    monkeypatch.setattr(
        sysobj, "_run_admin_sql", lambda admin, stmts: statements.extend(stmts)
    )

    res = sysobj.domain_result(_admin(), "ch_in", "dompw")
    sysobj.do_create(_admin(), res)
    joined = "\n".join(statements)

    assert 'CREATE DATABASE "ch_in"' in joined
    assert 'CREATE USER "ch_in"' in joined
    assert 'GRANT ALL ON "ch_in".* TO "ch_in"' in joined
    # The global privileges the s3()/gcs staged INSERT needs (azure, using
    # azureBlobStorage(), does not).
    assert 'GRANT S3, CREATE TEMPORARY TABLE ON *.* TO "ch_in"' in joined
