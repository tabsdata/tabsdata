#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for tabsdatax.itest.tdksystems.manifest — YAML batch/echo for create."""
from __future__ import annotations

import io

import pytest

from tabsdatax.itest.tdksystems import manifest
from tabsdatax.itest.tdksystems.errors import DatagenError


# ---- read ------------------------------------------------------------------

def test_read_list_of_entries(tmp_path):
    p = tmp_path / "m.yaml"
    p.write_text("- type: fs\n  sys: a\n- type: s3\n  sys: b\n")
    assert manifest.read(str(p)) == [
        {"type": "fs", "sys": "a"},
        {"type": "s3", "sys": "b"},
    ]


def test_read_empty_is_empty_list(tmp_path):
    p = tmp_path / "m.yaml"
    p.write_text("")
    assert manifest.read(str(p)) == []


def test_read_rejects_non_list(tmp_path):
    p = tmp_path / "m.yaml"
    p.write_text("type: fs\nsys: a\n")   # a mapping, not a list
    with pytest.raises(DatagenError, match="must be a YAML list"):
        manifest.read(str(p))


def test_read_missing_file(tmp_path):
    with pytest.raises(DatagenError, match="cannot read manifest"):
        manifest.read(str(tmp_path / "nope.yaml"))


def test_read_dash_reads_stdin(monkeypatch):
    monkeypatch.setattr("sys.stdin", io.StringIO("- type: fs\n  name: a\n"))
    assert manifest.read("-") == [{"type": "fs", "name": "a"}]


# ---- entry_to_argv ---------------------------------------------------------

def test_entry_to_argv_scalar_bool_env_and_number():
    argv = manifest.entry_to_argv({
        "type": "s3",
        "sys": "landing",
        "force": True,
        "bucket": "b",
        "region": {"env": "AWS_REGION"},
        "port": 5433,
        "ready-timeout": False,   # falsey bool -> omitted
    })
    assert argv == [
        "--type", "s3", "--sys", "landing", "--force", "--bucket", "b",
        "--env:region", "AWS_REGION", "--port", "5433",
    ]


def test_entry_to_argv_skips_none():
    assert manifest.entry_to_argv({"type": "fs", "sys": None}) == ["--type", "fs"]


def test_entry_to_argv_rejects_bad_mapping():
    with pytest.raises(DatagenError, match="env: VAR"):
        manifest.entry_to_argv({"region": {"literal": "x"}})


# ---- system_entry_from_descriptor (the `manifest` subcommand) ---------------

def test_system_entry_from_descriptor_fs():
    doc = {"system": "fs", "name": "local",
           "meta": [{"name": "SYS_PATH", "value": "/tmp/x/local"}]}
    assert manifest.system_entry_from_descriptor(doc) == {
        "type": "fs", "name": "local", "sys-path": "/tmp/x/local"
    }


def test_system_entry_from_descriptor_s3_literal_and_env():
    doc = {"system": "s3", "name": "landing", "meta": [
        {"name": "BUCKET", "value": "b"},
        # env-backed: value is baked, but the env marker re-emits the reference
        {"name": "REGION", "value": "us-east-1", "env": "AWS_REGION"},
        {"name": "ACCESS_KEY", "value": ""},           # empty -> omitted
        {"name": "SYS_PATH", "value": "root"},
    ]}
    entry = manifest.system_entry_from_descriptor(doc)
    assert entry["type"] == "s3" and entry["name"] == "landing"
    assert entry["bucket"] == "b"
    assert entry["region"] == {"env": "AWS_REGION"}
    assert entry["sys-path"] == "root"
    assert "access-key" not in entry and "secret-key" not in entry


def test_domain_entry_from_descriptor_name_sys_model_data():
    # name is the domain name verbatim; the entity prefix is intentionally omitted
    # (a manifest replays under each run's own prefix).
    doc = {"system": "fs", "name": "orders", "sys_name": "mysys",
           "model": "world", "data": "xs", "external_name": "w0_orders"}
    assert manifest.domain_entry_from_descriptor(doc) == {
        "name": "orders", "sys": "mysys", "model": "world", "data": "xs",
    }


def test_domain_entry_from_descriptor_empty_domain():
    doc = {"system": "fs", "name": "inbox", "sys_name": "mysys",
           "external_name": "u_0_inbox"}
    assert manifest.domain_entry_from_descriptor(doc) == {
        "name": "inbox", "sys": "mysys",
    }


# ---- write -----------------------------------------------------------------

def test_write_to_file_round_trips(tmp_path):
    entries = [{"type": "fs", "sys": "a", "sys-path": "/tmp/a"}]
    out = tmp_path / "m.yaml"
    manifest.write(str(out), entries)
    assert manifest.read(str(out)) == entries


def test_write_creates_missing_parent_dirs(tmp_path):
    entries = [{"type": "fs", "sys": "a"}]
    out = tmp_path / "config" / "nested" / "systems.yaml"
    manifest.write(str(out), entries)
    assert manifest.read(str(out)) == entries


def test_write_dash_goes_to_stdout(capsys):
    manifest.write("-", [{"type": "fs", "sys": "a"}])
    out = capsys.readouterr().out
    assert "type: fs" in out and "sys: a" in out
