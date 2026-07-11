"""SYS_PATH / BASE_PATH for object-store and file-staging systems must never
start with '/': `descriptor.resolve` strips a leading slash (with a warning on
the flagged holder), so the exported + delegated values are relative. The
local-filesystem (fs) backend is exempt — an absolute root is valid there."""
from __future__ import annotations

import json

from tabsdatax.itest.tdksystems import descriptor


def _resolved(path):
    return descriptor.resolve(descriptor.load(path), warn=True)


def _info(path, name):
    descriptor.stamp(path, name=name, started="t")
    return json.loads(descriptor.info(descriptor.load(path), "json"))


# ---- object-store domain: strip + warn -----------------------------------

def test_cloud_domain_strips_leading_slash_and_warns(tmp_path, capsys):
    out = tmp_path / "s3_OUT.json"
    descriptor.write_cloud_domain(
        out=str(out), storage_type="s3", system_kind="file",
        sys_path="/xp-s3", sys_path_env=None, base_path="/xp-s3/s3_OUT",
        forwarded=[("BUCKET", "b", None)])
    meta = _resolved(out)
    err = capsys.readouterr().err
    assert meta["SYS_PATH"] == "xp-s3"
    assert meta["BASE_PATH"] == "xp-s3/s3_OUT"
    assert "WARNING" in err and "must not start with" in err
    # Only SYS_PATH warns; BASE_PATH is corrected silently (single notice).
    assert err.count("WARNING") == 1
    info = _info(out, "s3_out")
    assert info["SYS_PATH"] == "xp-s3"
    assert info["BASE_PATH"] == "xp-s3/s3_OUT"


def test_cloud_domain_clean_value_is_silent(tmp_path, capsys):
    out = tmp_path / "s3_clean.json"
    descriptor.write_cloud_domain(
        out=str(out), storage_type="s3", system_kind="file",
        sys_path="xp-s3", sys_path_env=None, base_path="xp-s3/s3_clean",
        forwarded=[("BUCKET", "b", None)])
    assert _resolved(out)["SYS_PATH"] == "xp-s3"
    assert "WARNING" not in capsys.readouterr().err


def test_cloud_domain_strips_multiple_leading_slashes(tmp_path):
    out = tmp_path / "s3_OUT.json"
    descriptor.write_cloud_domain(
        out=str(out), storage_type="s3", system_kind="file",
        sys_path="///xp-s3", sys_path_env=None, base_path="///xp-s3/s3_OUT",
        forwarded=[])
    assert _resolved(out)["SYS_PATH"] == "xp-s3"


def test_cloud_system_strips_leading_slash(tmp_path, capsys):
    out = tmp_path / "s3.json"
    descriptor.write_cloud_system(
        out=str(out), storage_type="s3", system_kind="file", label="s3",
        sys_path="/xp-s3", sys_path_env=None,
        forwarded=[("BUCKET", "b", None)])
    assert _resolved(out)["SYS_PATH"] == "xp-s3"
    assert "WARNING" in capsys.readouterr().err


def test_cloud_domain_env_backed_value_is_stripped(tmp_path, capsys):
    # An env-backed SYS_PATH bakes its value at create, so its leading slash is
    # stripped at resolve just like a literal's; the env marker survives on the
    # raw descriptor for a manifest dump.
    out = tmp_path / "s3_OUT.json"
    descriptor.write_cloud_domain(
        out=str(out), storage_type="s3", system_kind="file",
        sys_path="/xp-s3", sys_path_env="S3__SYS_PATH", base_path="/xp-s3/s3_OUT",
        forwarded=[])
    assert _resolved(out)["SYS_PATH"] == "xp-s3"
    assert "WARNING" in capsys.readouterr().err
    raw = {e["name"]: e.get("env") for e in descriptor.load(out)["meta"]}
    assert raw["SYS_PATH"] == "S3__SYS_PATH"


# ---- fs is exempt (absolute roots are valid) ------------------------------

def test_fs_domain_keeps_absolute_path(tmp_path, capsys):
    out = tmp_path / "fs_OUT.json"
    descriptor.write_cloud_domain(
        out=str(out), storage_type="fs", system_kind="file",
        sys_path="/tmp/tdx/xp-fs", sys_path_env=None,
        base_path="/tmp/tdx/xp-fs/fs_OUT", forwarded=[],
        strip_sys_path_slash=False)
    meta = _resolved(out)
    assert meta["SYS_PATH"] == "/tmp/tdx/xp-fs"
    assert meta["BASE_PATH"] == "/tmp/tdx/xp-fs/fs_OUT"
    assert "WARNING" not in capsys.readouterr().err


def test_fs_system_marks_sys_path_non_relative():
    from tabsdatax.itest.tdksystems.systems.fs.system import FsSystem
    from tabsdatax.itest.tdksystems.systems.s3.system import S3System
    assert FsSystem.relative_sys_path is False
    assert S3System.relative_sys_path is True


# ---- file-staging container domains strip too -----------------------------

def test_staging_container_domain_strips_leading_slash(tmp_path, capsys):
    out = tmp_path / "clickhouse_OUT.json"
    descriptor.write_container_domain(
        out=str(out), system_type="clickhouse", system_kind="sql",
        host="localhost", port="8123", user="u", password="p",
        db="clickhouse_OUT", sqlalchemy_uri="clickhouse+http://x",
        domain_desc="db", supports_sql=True,
        sys_path="/xp-staging", sys_path_env=None,
        base_path="/xp-staging/clickhouse_OUT")
    meta = _resolved(out)
    assert meta["SYS_PATH"] == "xp-staging"
    assert meta["BASE_PATH"] == "xp-staging/clickhouse_OUT"
    assert "WARNING" in capsys.readouterr().err


def test_staging_container_system_strips_leading_slash(tmp_path, capsys):
    out = tmp_path / "clickhouse.json"
    descriptor.write_container_system(
        out=str(out), system_type="clickhouse", system_kind="sql",
        label="ClickHouse", container="clickhouse", host="localhost",
        port="8123", user="default", password="pw", domain_desc="db",
        sys_path="/xp-staging", sys_path_env=None)
    assert _resolved(out)["SYS_PATH"] == "xp-staging"
    assert "WARNING" in capsys.readouterr().err
