#
# Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the 1Password secret provider (`tabsdatax.itest.tdksystems.secret_1p`).

We exercise the parts that don't need the real `op` CLI: name normalisation,
the missing-CLI error path, and `fetch()` with `subprocess.run` stubbed to
mimic `op item list` / `op item get`."""
from __future__ import annotations

import json
import subprocess

import pytest

from tabsdatax.itest.tdksystems import secret_1p
from tabsdatax.itest.tdksystems.errors import DatagenError


# ---- _norm — same rules as the old bash __1p_norm -------------------------

@pytest.mark.parametrize("raw,norm", [
    ("fs",           "FS"),
    ("FS",           "FS"),
    ("sys_path",     "SYS_PATH"),
    ("sys-path",     "SYS_PATH"),
    ("Mixed.Name 1", "MIXED_NAME_1"),
    ("123abc",       "_123ABC"),
    ("",             ""),
    ("IAM user",     "IAM_USER"),
])
def test_norm(raw, norm):
    assert secret_1p._norm(raw) == norm


# ---- missing `op` CLI -----------------------------------------------------

def test_fetch_errors_clearly_when_op_missing(monkeypatch):
    real_run = subprocess.run

    def fake_run(args, *a, **kw):
        if args[:2] == ["which", "op"]:
            return subprocess.CompletedProcess(args, returncode=1, stdout="", stderr="")
        return real_run(args, *a, **kw)

    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(DatagenError, match="'op' not found"):
        secret_1p.fetch("dev_xp")


# ---- fetch — `op` stubbed -------------------------------------------------

def _stub_op(monkeypatch, *, items, get):
    """Stub subprocess.run so `which op` succeeds, `item list` returns `items`,
    and `item get <title>` returns get[title] (a dict) or fails if missing."""
    def fake_run(args, *a, **kw):
        if args[:2] == ["which", "op"]:
            return subprocess.CompletedProcess(args, 0, "", "")
        if "list" in args:
            return subprocess.CompletedProcess(args, 0, json.dumps(items), "")
        if "get" in args:
            title = args[args.index("get") + 1]
            if title in get:
                return subprocess.CompletedProcess(args, 0, json.dumps(get[title]), "")
            return subprocess.CompletedProcess(args, 1, "", "not found")
        raise AssertionError(f"unexpected op call: {args}")
    monkeypatch.setattr(subprocess, "run", fake_run)


def test_fetch_builds_item_field_dict(monkeypatch):
    _stub_op(
        monkeypatch,
        items=[{"title": "SNOWFLAKE"}, {"title": "S3"}],
        get={
            "SNOWFLAKE": {"title": "SNOWFLAKE", "fields": [
                {"id": "account", "value": "acct"},
                {"id": "pat", "value": "tok"},
                {"id": "expires", "value": "2030"},      # skipped
                {"id": "note", "value": None},           # skipped (null)
            ]},
            "S3": {"title": "S3", "fields": [
                {"id": "bucket", "value": "b"},
            ]},
        },
    )
    out = secret_1p.fetch("dev_xp")
    assert out == {
        "SNOWFLAKE__ACCOUNT": "acct",
        "SNOWFLAKE__PAT": "tok",
        "S3__BUCKET": "b",
    }


def test_fetch_silently_skips_unreadable_items(monkeypatch, capsys):
    _stub_op(
        monkeypatch,
        items=[{"title": "GOOD"}, {"title": "LOCKED"}],
        get={"GOOD": {"title": "GOOD", "fields": [{"id": "k", "value": "v"}]}},
    )
    out = secret_1p.fetch("dev_xp")
    assert out == {"GOOD__K": "v"}   # the unreadable item is skipped
    # fetch() prints nothing — no stdout, no stderr chatter.
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == ""


def test_fetch_empty_vault_errors(monkeypatch):
    _stub_op(monkeypatch, items=[], get={})
    with pytest.raises(DatagenError, match="returned no items"):
        secret_1p.fetch("dev_xp")


def test_fetch_list_failure_errors(monkeypatch):
    def fake_run(args, *a, **kw):
        if args[:2] == ["which", "op"]:
            return subprocess.CompletedProcess(args, 0, "", "")
        return subprocess.CompletedProcess(args, 1, "", "auth required")
    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(DatagenError, match="failed to list items"):
        secret_1p.fetch("dev_xp")
