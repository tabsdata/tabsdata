"""`tdx-system` / `tdx-domain` never rewrite a given path: SYS_PATH / BASE_PATH
are stored and resolved exactly as given — leading, trailing and inner slashes
are all preserved, for object stores, file-staging containers and fs alike.
`_validate_path` only guards (single line, no leading/trailing whitespace); it
never modifies the value."""
from __future__ import annotations

import json

import pytest

from tabsdatax.itest.tdksystems import descriptor
from tabsdatax.itest.tdksystems.errors import DatagenError
from tabsdatax.itest.tdksystems.systems.base import _validate_path


def _resolved(path):
    return descriptor.resolve(descriptor.load(path))


def _info(path, name):
    descriptor.stamp(path, name=name, started="t")
    return json.loads(descriptor.info(descriptor.load(path), "json"))


# ---- object stores: value preserved verbatim -----------------------------

def test_cloud_domain_preserves_leading_slash(tmp_path, capsys):
    out = tmp_path / "s3_OUT.json"
    descriptor.write_cloud_domain(
        out=str(out), storage_type="s3", system_kind="file",
        sys_path="/xp-s3", sys_path_env=None, base_path="/xp-s3/s3_OUT",
        forwarded=[("BUCKET", "b", None)])
    meta = _resolved(out)
    assert meta["SYS_PATH"] == "/xp-s3"
    assert meta["BASE_PATH"] == "/xp-s3/s3_OUT"
    assert capsys.readouterr().err == ""
    info = _info(out, "s3_out")
    assert info["SYS_PATH"] == "/xp-s3"
    assert info["BASE_PATH"] == "/xp-s3/s3_OUT"


def test_cloud_domain_preserves_trailing_and_multiple_slashes(tmp_path):
    out = tmp_path / "s3_OUT.json"
    descriptor.write_cloud_domain(
        out=str(out), storage_type="s3", system_kind="file",
        sys_path="///xp-s3/", sys_path_env=None, base_path="///xp-s3//s3_OUT",
        forwarded=[])
    meta = _resolved(out)
    assert meta["SYS_PATH"] == "///xp-s3/"
    assert meta["BASE_PATH"] == "///xp-s3//s3_OUT"


def test_cloud_system_preserves_leading_slash(tmp_path, capsys):
    out = tmp_path / "s3.json"
    descriptor.write_cloud_system(
        out=str(out), storage_type="s3", system_kind="file", label="s3",
        sys_path="/xp-s3", sys_path_env=None,
        forwarded=[("BUCKET", "b", None)])
    assert _resolved(out)["SYS_PATH"] == "/xp-s3"
    assert capsys.readouterr().err == ""


def test_cloud_domain_env_backed_value_preserved(tmp_path):
    out = tmp_path / "s3_OUT.json"
    descriptor.write_cloud_domain(
        out=str(out), storage_type="s3", system_kind="file",
        sys_path="/xp-s3", sys_path_env="S3__SYS_PATH", base_path="/xp-s3/s3_OUT",
        forwarded=[])
    assert _resolved(out)["SYS_PATH"] == "/xp-s3"
    raw = {e["name"]: e.get("env") for e in descriptor.load(out)["meta"]}
    assert raw["SYS_PATH"] == "S3__SYS_PATH"


# ---- fs: absolute roots preserved (as before) -----------------------------

def test_fs_domain_keeps_absolute_path(tmp_path, capsys):
    out = tmp_path / "fs_OUT.json"
    descriptor.write_cloud_domain(
        out=str(out), storage_type="fs", system_kind="file",
        sys_path="/tmp/tdx/xp-fs", sys_path_env=None,
        base_path="/tmp/tdx/xp-fs/fs_OUT", forwarded=[])
    meta = _resolved(out)
    assert meta["SYS_PATH"] == "/tmp/tdx/xp-fs"
    assert meta["BASE_PATH"] == "/tmp/tdx/xp-fs/fs_OUT"
    assert capsys.readouterr().err == ""


# ---- file-staging container domains preserve too --------------------------

def test_staging_container_domain_preserves_leading_slash(tmp_path):
    out = tmp_path / "clickhouse_OUT.json"
    descriptor.write_container_domain(
        out=str(out), system_type="clickhouse", system_kind="sql",
        host="localhost", port="8123", user="u", password="p",
        db="clickhouse_OUT", sqlalchemy_uri="clickhouse+http://x",
        domain_desc="db", supports_sql=True,
        sys_path="/xp-staging", sys_path_env=None,
        base_path="/xp-staging/clickhouse_OUT")
    meta = _resolved(out)
    assert meta["SYS_PATH"] == "/xp-staging"
    assert meta["BASE_PATH"] == "/xp-staging/clickhouse_OUT"


def test_staging_container_system_preserves_leading_slash(tmp_path):
    out = tmp_path / "clickhouse.json"
    descriptor.write_container_system(
        out=str(out), system_type="clickhouse", system_kind="sql",
        label="ClickHouse", container="clickhouse", host="localhost",
        port="8123", user="default", password="pw", domain_desc="db",
        sys_path="/xp-staging", sys_path_env=None)
    assert _resolved(out)["SYS_PATH"] == "/xp-staging"


# ---- _validate_path guards only, never modifies ---------------------------

@pytest.mark.parametrize("ok", ["/xp-s3", "xp-s3/", "///a//b/", "/tmp/tdx/x"])
def test_validate_path_accepts_slashes_verbatim(ok):
    _validate_path(ok, "sys-path")  # no exception, no rewrite


@pytest.mark.parametrize("bad", [" /xp-s3", "/xp-s3 ", "a\nb", "a\tb "])
def test_validate_path_rejects_whitespace_and_multiline(bad):
    with pytest.raises(DatagenError):
        _validate_path(bad, "sys-path")
