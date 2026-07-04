"""Integration-test shared fixtures.

Tests in this directory require external resources (Docker, network,
1Password CLI, …) and are NOT run by the default `pytest` invocation.
Run them explicitly with `pytest src/itest/python` (or `-m integration`).

All test artefacts (generated JSON descriptors, fs roots, data dirs)
land under `target/itest/<test-id>/` so they're trivially cleanable
with `rm -rf target`.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest


# The tests do `from conftest import ...`; under the toolkit's
# `--import-mode=importlib` the test folder is not on sys.path, so make this
# directory importable by name.
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

# Resolve the project root from this conftest's location: src/itest/python/tdksystems/conftest.py.
PROJECT_ROOT = Path(__file__).resolve().parents[4]
TARGET = PROJECT_ROOT / "target"
ITEST_ROOT = TARGET / "itest"


@pytest.fixture(scope="session", autouse=True)
def _ensure_itest_root_exists():
    ITEST_ROOT.mkdir(parents=True, exist_ok=True)
    yield


@pytest.fixture(autouse=True)
def _td_dev_secret(monkeypatch):
    """Descriptors are encrypted with $TD_DEV_PERSONAL_SECRET, so tdx-system / tdx-domain
    need it set. Provide one for the suite unless the environment already has
    a (valid) one — so the descriptors the tests create can be read back."""
    if len(os.environ.get("TD_DEV_PERSONAL_SECRET", "")) < 12:
        monkeypatch.setenv("TD_DEV_PERSONAL_SECRET", "itest-dev-secret")


@pytest.fixture
def itest_dir(request) -> Path:
    """A fresh per-test directory under `target/itest/`."""
    name = request.node.name.replace("[", "_").replace("]", "").replace("/", "_")
    p = ITEST_ROOT / name
    if p.exists():
        shutil.rmtree(p)
    p.mkdir(parents=True)
    return p


def have_cmd(name: str) -> bool:
    return subprocess.run(
        ["which", name], capture_output=True
    ).returncode == 0


def have_docker_running() -> bool:
    if not have_cmd("docker"):
        return False
    return subprocess.run(
        ["docker", "info"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    ).returncode == 0


def have_op_vault(vault: str) -> bool:
    """Return True iff the `op` CLI is installed and the named vault is
    accessible (we don't require any specific item in it — that's tested
    by `test_cloud_systems.py` per cloud)."""
    if not have_cmd("op"):
        return False
    return subprocess.run(
        ["op", "--vault", vault, "item", "list", "--format=json"],
        capture_output=True
    ).returncode == 0


def run_cmd(*args, env=None, check=False) -> subprocess.CompletedProcess:
    """Wrapper around subprocess.run that captures stdout+stderr as text."""
    return subprocess.run(args, capture_output=True, text=True, env=env, check=check)


def assert_ok(cp: subprocess.CompletedProcess, label: str) -> None:
    assert cp.returncode == 0, (
        f"{label} failed (rc={cp.returncode})\n"
        f"--- stdout ---\n{cp.stdout}\n"
        f"--- stderr ---\n{cp.stderr}"
    )


def source_1p_env(vault: str) -> dict:
    """Run `tdx-system secrets --get 1p:<vault> --out export` and return the
    resulting env dict (current env + the exports). For test convenience:
    tests that interact with cloud systems pass this as `env=` to subprocess
    calls."""
    cp = run_cmd("tdx-system", "secrets", "--get", f"1p:{vault}", "--out", "export")
    if cp.returncode != 0:
        raise RuntimeError(
            f"tdx-system secrets --get 1p:{vault} --out export failed: {cp.stderr}"
        )
    extra = {}
    for line in cp.stdout.splitlines():
        line = line.strip()
        if not line.startswith("export "):
            continue
        kv = line[len("export "):]
        if "=" not in kv:
            continue
        k, _, v = kv.partition("=")
        # Strip surrounding quotes that shlex.quote may have added.
        v = v.strip()
        if v.startswith("'") and v.endswith("'"):
            v = v[1:-1].replace("'\\''", "'")
        extra[k] = v
    out = os.environ.copy()
    out.update(extra)
    return out
