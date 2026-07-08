"""Integration tests: every cloud-storage system end-to-end.

Cloud creds come from a 1Password vault — by default `dev_xp`. Tests
do `eval $(tdx-system secrets --get 1p:dev_xp --out export)` (in-process
via the helper in conftest) and rely on each system's default
`--env-prefix <SYS>` to derive its options from the resulting
`<SYS>__<FIELD>` env vars (the naming that provider produces).

Skipped when:
  - the `op` CLI isn't installed,
  - the vault isn't accessible,
  - the per-cloud item isn't present (test-by-test).

All test artefacts land under `target/itest/<test-id>/`."""
from __future__ import annotations

from pathlib import Path

import pytest

from tabsdatax.itest.tdksystems import systems

from conftest import (
    assert_ok, have_op_vault, have_cmd, run_cmd, source_1p_env,
)

pytestmark = pytest.mark.integration


VAULT = "dev_xp"


@pytest.fixture(scope="module")
def vault_env():
    if not have_op_vault(VAULT):
        pytest.skip(f"1Password vault '{VAULT}' not accessible (op CLI / login)")
    if not have_cmd("tdx-system"):
        pytest.skip("tdx-system not on PATH (run: pip install -e .)")
    try:
        return source_1p_env(VAULT)
    except RuntimeError as exc:
        pytest.skip(str(exc))


# ---- fs (local filesystem) — does NOT need 1p; sanity baseline -----------

def test_fs_system_lifecycle_no_creds_needed(itest_dir):
    fs_root = itest_dir / "fs-root"
    sysdir, domdir = itest_dir / "systems", itest_dir / "domains"
    S = ("--sys-dir", str(sysdir))
    D = ("--domain-dir", str(domdir))

    cp = run_cmd("tdx-system", "create", "--type", "fs", "--sys", "fs_sys", *S,
                 "--sys-path", str(fs_root), "--force")
    assert_ok(cp, "tdx-system create fs")

    # Create the domain and load the `world` model's data (CSV) into it.
    cp = run_cmd("tdx-domain", "create", "--sys", "fs_sys", *S,
                 "--domain", "inbox", *D,
                 "--model", "world", "--data", "xs", "--force")
    assert_ok(cp, "create fs domain + data")
    assert (fs_root / "inbox" / "wld_city.csv").is_file()

    cp = run_cmd("tdx-domain", "info", "--domain", "inbox", *D, "--format", "json")
    assert_ok(cp, "fs domain info")

    assert_ok(run_cmd("tdx-domain", "destroy", "--domain", "inbox", *D),
              "fs domain destroy")
    assert not (fs_root / "inbox").exists()
    assert_ok(run_cmd("tdx-system", "destroy", "--sys", "fs_sys", *S),
              "fs system destroy")


# ---- s3 / gcs / azureblob — pull creds from 1p ---------------------------

CLOUD_PARAMS = [
    pytest.param("s3",        "S3",         id="s3"),
    pytest.param("gcs",       "GCS",        id="gcs"),
    pytest.param("azureblob", "AZ",         id="azureblob"),
]


@pytest.mark.parametrize("slug,prefix", CLOUD_PARAMS)
def test_cloud_system_lifecycle(slug, prefix, vault_env, itest_dir):
    """For each object-store backend: create-system → status → create-domain
    (+ load data) → info → destroy. Reads creds from 1p via
    `tdx-system secrets`."""
    # Skip if the matching 1p item didn't populate every var the cloud
    # needs. `--env-prefix` appends '__' before the option name (matching
    # the `<TITLE>__<FIELD>` naming `tdx-system secrets` emits), so each option
    # OPT becomes $<PREFIX>__<OPT>.
    sys_obj = systems.get(slug)
    needed = [o.name for o in sys_obj.options] + ["SYS_PATH"]
    missing = [f"{prefix}__{k}" for k in needed if f"{prefix}__{k}" not in vault_env]
    if missing:
        pytest.skip(f"vault {VAULT!r} missing 1p item for {slug}: {missing}")

    sysdir, domdir = itest_dir / "systems", itest_dir / "domains"
    S = ("--sys-dir", str(sysdir))
    D = ("--domain-dir", str(domdir))
    sysname = f"{slug}_sys"

    cp = run_cmd(
        "tdx-system", "create", "--type", slug,
        "--sys", sysname, *S, "--env-prefix", prefix, "--force",
        env=vault_env,
    )
    assert_ok(cp, f"tdx-system create {slug} --env-prefix {prefix}")
    assert (sysdir / sysname).is_file()

    try:
        cp = run_cmd("tdx-system", "status", "--sys", sysname, *S, env=vault_env)
        assert_ok(cp, f"{slug} status")
        assert "ready" in cp.stdout.lower()

        cp = run_cmd("tdx-domain", "create", "--sys", sysname, *S,
                     "--domain", "inbox", *D,
                     "--model", "world", "--data", "xs", "--force",
                     env=vault_env)
        assert_ok(cp, f"{slug} create domain + data")
        assert (domdir / "inbox").is_file()

        cp = run_cmd("tdx-domain", "info", "--domain", "inbox", *D,
                     "--format", "json", env=vault_env)
        assert_ok(cp, f"{slug} domain info")
    finally:
        # Tear down whatever we wrote to the prefix, then the system.
        run_cmd("tdx-domain", "destroy", "--domain", "inbox", *D, env=vault_env)
        run_cmd("tdx-system", "destroy", "--sys", sysname, *S, env=vault_env)
