"""Integration test: full lifecycle of an `fs` system + domain via the tdx CLI.

Self-contained — uses a tmp directory under /tmp (so it satisfies fs's
safe-root validation) and shells out to the installed `tdx-system` / `tdx-domain`
commands."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def fs_root(tmp_path):
    """Pick a /tmp-rooted dir (fs validates that absolute paths start with
    /tmp, /home or /Users). Clean up after."""
    p = Path(f"/tmp/tdx-itest-{os.getpid()}-{tmp_path.name}")
    yield p
    if p.exists():
        shutil.rmtree(p, ignore_errors=True)


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(args, capture_output=True, text=True, check=False)


def _ok(cp: subprocess.CompletedProcess, msg: str) -> None:
    assert cp.returncode == 0, f"{msg}\nstdout:\n{cp.stdout}\nstderr:\n{cp.stderr}"


def test_full_fs_lifecycle(fs_root, tmp_path):
    # Systems/domains are addressed by name in a store dir; point both stores at
    # per-test tmp dirs with --sys-dir / --domain-dir.
    sysdir, domdir = tmp_path / "systems", tmp_path / "domains"
    S = ("--sys-dir", str(sysdir))
    D = ("--domain-dir", str(domdir))
    sysd, domd = sysdir / "my_fs", domdir / "inbox"

    # 1. Create the fs system.
    cp = _run(
        "tdx-system",
        "create",
        "--type",
        "fs",
        "--sys",
        "my_fs",
        *S,
        "--sys-path",
        str(fs_root),
        "--force",
    )
    _ok(cp, "tdx-system create fs")
    assert sysd.is_file() and fs_root.is_dir()

    # 2. status + info (json / export / unset).
    _ok(_run("tdx-system", "status", "--sys", "my_fs", *S), "sys status")
    cp = _run("tdx-system", "info", "--sys", "my_fs", *S, "--format", "json")
    _ok(cp, "sys info json")
    info = json.loads(cp.stdout)
    assert info["TYPE"] == "fs"
    assert info["SYS_PATH"] == str(fs_root)

    # list shows the system + its type.
    cp = _run("tdx-system", "list", *S)
    _ok(cp, "sys list")
    assert "my_fs" in cp.stdout and "fs" in cp.stdout

    # 3. Create a domain and load the `world` model's data into it.
    cp = _run(
        "tdx-domain",
        "create",
        "--sys",
        "my_fs",
        *S,
        "--domain",
        "inbox",
        *D,
        "--model",
        "world",
        "--data",
        "xs",
        "--force",
    )
    _ok(cp, "tdx-domain create + data")
    assert domd.is_file()
    dom_root = fs_root / "inbox"
    assert dom_root.is_dir()
    assert (dom_root / "wld_city.csv").is_file()

    cp = _run("tdx-domain", "info", "--domain", "inbox", *D, "--format", "json")
    _ok(cp, "domain info json")
    dinfo = json.loads(cp.stdout)
    assert dinfo["MODEL"] == "world"
    assert dinfo["DATA"] == "xs"
    assert dinfo["BASE_PATH"] == f"{fs_root}/inbox"
    _ok(_run("tdx-domain", "status", "--domain", "inbox", *D), "domain status")

    # list shows the domain with its type + model/data.
    cp = _run("tdx-domain", "list", *D)
    _ok(cp, "domain list")
    assert (
        "inbox" in cp.stdout and "model=world" in cp.stdout and "data=xs" in cp.stdout
    )

    # 4. Destroy the domain (removes objects + descriptor).
    _ok(_run("tdx-domain", "destroy", "--domain", "inbox", *D), "domain destroy")
    assert not domd.exists()
    assert not dom_root.exists()

    # 5. Destroy the system (removes the root + descriptor).
    _ok(_run("tdx-system", "destroy", "--sys", "my_fs", *S), "sys destroy")
    assert not sysd.exists()
    assert not fs_root.exists()
