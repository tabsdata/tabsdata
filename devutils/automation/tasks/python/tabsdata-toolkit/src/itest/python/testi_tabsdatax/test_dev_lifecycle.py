#
# Copyright 2026 Tabsdata Inc.
#

"""Integration test: full lifecycle of a `dev` (no-op) system + domain via the
tdx CLI.

The dev backend has no external resource, so this is purely a descriptor
lifecycle: create / status / info / list / destroy for both the system and its
domain, plus the guard that it rejects --model / --data (supports_models is
False). Self-contained — points the stores at a per-test tmp dir and shells out
to the installed `tdx-system` / `tdx-domain` commands."""

from __future__ import annotations

import json

import pytest
from conftest import assert_ok, run_cmd

pytestmark = pytest.mark.integration


def test_full_dev_lifecycle(itest_dir):
    sysdir, domdir = itest_dir / "systems", itest_dir / "domains"
    sysd, domd = sysdir / "my_dev", domdir / "scratch"

    # 1. Create the dev system (no config, no external resource).
    assert_ok(
        run_cmd(
            "tdx-system",
            "create",
            "--type",
            "dev",
            "--sys",
            "my_dev",
            "--sys-dir",
            str(sysdir),
            "--force",
        ),
        "tdx-system create dev",
    )
    assert sysd.is_file()

    # 2. status + info json.
    assert_ok(
        run_cmd("tdx-system", "status", "--sys", "my_dev", "--sys-dir", str(sysdir)),
        "sys status",
    )
    cp = run_cmd(
        "tdx-system",
        "info",
        "--sys",
        "my_dev",
        "--sys-dir",
        str(sysdir),
        "--format",
        "json",
    )
    assert_ok(cp, "sys info json")
    info = json.loads(cp.stdout)
    assert info["TYPE"] == "dev"
    assert info["SYSTEM_KIND"] == "dev"

    # list shows the system + its type.
    cp = run_cmd("tdx-system", "list", "--sys-dir", str(sysdir))
    assert_ok(cp, "sys list")
    assert "my_dev" in cp.stdout and "dev" in cp.stdout

    # 3. Create a domain — no external entity, no model/data. `--domain-prefix ""`
    #    keeps the logical name bare.
    assert_ok(
        run_cmd(
            "tdx-domain",
            "create",
            "--sys",
            "my_dev",
            "--sys-dir",
            str(sysdir),
            "--domain",
            "scratch",
            "--domain-dir",
            str(domdir),
            "--domain-prefix",
            "",
            "--force",
        ),
        "tdx-domain create dev",
    )
    assert domd.is_file()

    cp = run_cmd(
        "tdx-domain",
        "info",
        "--domain",
        "scratch",
        "--domain-dir",
        str(domdir),
        "--format",
        "json",
    )
    assert_ok(cp, "domain info json")
    dinfo = json.loads(cp.stdout)
    assert dinfo["TYPE"] == "dev"
    assert dinfo["SYSTEM_KIND"] == "dev"
    assert_ok(
        run_cmd(
            "tdx-domain", "status", "--domain", "scratch", "--domain-dir", str(domdir)
        ),
        "domain status",
    )

    # 4. --model / --data are rejected (the dev backend has no data plane).
    cp = run_cmd(
        "tdx-domain",
        "create",
        "--sys",
        "my_dev",
        "--sys-dir",
        str(sysdir),
        "--domain",
        "bad",
        "--domain-dir",
        str(domdir),
        "--model",
        "world",
    )
    assert cp.returncode != 0
    assert "does not support --model" in (cp.stdout + cp.stderr)

    # 5. Destroy the domain, then the system (both just remove the descriptor).
    assert_ok(
        run_cmd(
            "tdx-domain", "destroy", "--domain", "scratch", "--domain-dir", str(domdir)
        ),
        "domain destroy",
    )
    assert not domd.exists()
    assert_ok(
        run_cmd("tdx-system", "destroy", "--sys", "my_dev", "--sys-dir", str(sysdir)),
        "sys destroy",
    )
    assert not sysd.exists()
