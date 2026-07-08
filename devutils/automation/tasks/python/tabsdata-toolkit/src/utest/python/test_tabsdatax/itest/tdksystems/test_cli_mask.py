"""Credentials are never echoed to the terminal by `tdx-system`/domain create —
passwords and the password inside a sqlalchemy URI are masked."""
from __future__ import annotations

from tabsdatax.itest.tdksystems import cli


def test_mask_secret():
    assert cli._mask_secret("FSZTcizv8193AHB9AogId98N") == "(set)"
    assert cli._mask_secret("") == ""
    assert cli._mask_secret(None) == ""


def test_mask_uri_clickhouse():
    uri = ("clickhouse+http://D_20260615T193831Z_tucu_O6:FSZTcizv8193AHB9AogId98N"
           "@localhost:54685/D_20260615T193831Z_tucu_O6")
    masked = cli._mask_uri(uri)
    assert "FSZTcizv8193AHB9AogId98N" not in masked
    assert masked == ("clickhouse+http://D_20260615T193831Z_tucu_O6:(set)"
                      "@localhost:54685/D_20260615T193831Z_tucu_O6")


def test_mask_uri_keeps_db2_query():
    uri = "db2+ibm_db://db2inst1:VMURdpjt20682@localhost:49160/XP?currentSchema=FOO"
    masked = cli._mask_uri(uri)
    assert "VMURdpjt20682" not in masked
    assert masked.endswith("@localhost:49160/XP?currentSchema=FOO")
    assert ":(set)@" in masked


def test_mask_uri_without_password_unchanged():
    # No user:pass@ component -> nothing to mask.
    assert cli._mask_uri("kafka://localhost:9092") == "kafka://localhost:9092"
    assert cli._mask_uri("") == ""
