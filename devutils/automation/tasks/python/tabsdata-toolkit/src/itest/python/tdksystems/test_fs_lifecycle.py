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
    sysd = tmp_path / "fs.json"
    domd = tmp_path / "inbox.json"

    # 1. Create the fs system.
    cp = _run("tdx-system", "create", "--type", "fs", "--sys", str(sysd),
              "--name", "my_fs", "--sys-path", str(fs_root), "--force")
    _ok(cp, "tdx-system create fs")
    assert sysd.is_file() and fs_root.is_dir()

    # 2. status + info (json / export / unset).
    _ok(_run("tdx-system", "status", "--sys", str(sysd)), "sys status")
    cp = _run("tdx-system", "info", "--sys", str(sysd), "--format", "json")
    _ok(cp, "sys info json")
    info = json.loads(cp.stdout)
    assert info["MY_FS__TYPE"] == "fs"
    assert info["MY_FS__SYS_PATH"] == str(fs_root)

    # 3. Create a domain and load the `world` model's data into it.
    cp = _run("tdx-domain", "create", "--sys", str(sysd), "--domain", str(domd),
              "--name", "inbox", "--model", "world", "--data", "xs", "--force")
    _ok(cp, "tdx-domain create + data")
    assert domd.is_file()
    dom_root = fs_root / "inbox"
    assert dom_root.is_dir()
    assert (dom_root / "wld_city.csv").is_file()

    cp = _run("tdx-domain", "info", "--domain", str(domd), "--format", "json")
    _ok(cp, "domain info json")
    dinfo = json.loads(cp.stdout)
    assert dinfo["INBOX__MODEL"] == "world"
    assert dinfo["INBOX__DATA"] == "xs"
    assert dinfo["INBOX__BASE_PATH"] == f"{fs_root}/inbox"
    _ok(_run("tdx-domain", "status", "--domain", str(domd)), "domain status")

    # 4. Destroy the domain (removes objects + descriptor).
    _ok(_run("tdx-domain", "destroy", "--domain", str(domd)), "domain destroy")
    assert not domd.exists()
    assert not dom_root.exists()

    # 5. Destroy the system (removes the root + descriptor).
    _ok(_run("tdx-system", "destroy", "--sys", str(sysd)), "sys destroy")
    assert not sysd.exists()
    assert not fs_root.exists()
