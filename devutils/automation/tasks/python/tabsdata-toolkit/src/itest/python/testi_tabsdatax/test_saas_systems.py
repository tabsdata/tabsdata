#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for saas systems: databricks / snowflake / salesforce.

Mirrors test_cloud_systems.py but with schema-based domain semantics:
  - databricks/snowflake: `python SYS create --schema NAME` actually creates
    the schema; the test runs status → list → create → list → delete.
  - salesforce: no remote action; only verifies that `python SYS create` writes
    the domain script and that its `--export` prints the expected vars.

Per-system creds come from the same `dev_xp` 1Password vault used by the
cloud tests, with `--env-prefix <SYS>` (resolves to `<SYS>__<FIELD>`).

Skipped when:
  - the `op` CLI / vault isn't accessible,
  - the per-system 1Password item isn't fully populated."""

from __future__ import annotations

import secrets

import pytest
from conftest import (
    assert_ok,
    have_cmd,
    have_op_vault,
    run_cmd,
    source_1p_env,
)

from tabsdatax.itest.tdksystems import systems

pytestmark = pytest.mark.integration


VAULT = "dev_xp"


@pytest.fixture(scope="module")
def vault_env():
    if not have_op_vault(VAULT):
        pytest.skip(f"1Password vault '{VAULT}' not accessible")
    if not have_cmd("tdx-system"):
        pytest.skip("tdx-system not on PATH (run: pip install -e .)")
    try:
        return source_1p_env(VAULT)
    except RuntimeError as exc:
        pytest.skip(str(exc))


SAAS_PARAMS = [
    pytest.param("databricks", "DATABRICKS", id="databricks"),
    pytest.param("snowflake", "SNOWFLAKE", id="snowflake"),
    pytest.param("salesforce", "SALESFORCE", id="salesforce"),
]


def _required_env_vars(slug: str, prefix: str) -> list[str]:
    """Every option the saas system declares becomes $<PREFIX>__<OPT>."""
    sys_obj = systems.get(slug)
    return [f"{prefix}__{o.name}" for o in sys_obj.options]


@pytest.mark.parametrize("slug,prefix", SAAS_PARAMS)
def test_saas_system_lifecycle(slug, prefix, vault_env, itest_dir):
    """create-system → status → create-domain (schema, or creds clone for
    salesforce) → info → destroy."""
    needed = _required_env_vars(slug, prefix)
    missing = [v for v in needed if v not in vault_env]
    if missing:
        pytest.skip(f"vault {VAULT!r} missing 1p item for {slug}: {missing}")

    sysdir, domdir = itest_dir / "systems", itest_dir / "domains"
    S = ("--sys-dir", str(sysdir))
    D = ("--domain-dir", str(domdir))
    sysname = f"{slug}_sys"

    cp = run_cmd(
        "tdx-system",
        "create",
        "--type",
        slug,
        "--sys",
        sysname,
        *S,
        "--env-prefix",
        prefix,
        "--force",
        env=vault_env,
    )
    assert_ok(cp, f"tdx-system create {slug} --env-prefix {prefix}")
    assert (sysdir / sysname).is_file()

    cp = run_cmd("tdx-system", "status", "--sys", sysname, *S, env=vault_env)
    assert_ok(cp, f"{slug} status")
    assert "ready" in cp.stdout.lower()

    # A random suffix so concurrent runs / leaked state can't collide.
    name = f"itest_smoke_{secrets.token_hex(4)}"
    try:
        cp = run_cmd(
            "tdx-domain",
            "create",
            "--sys",
            sysname,
            *S,
            "--domain",
            name,
            *D,
            "--force",
            env=vault_env,
        )
        assert_ok(cp, f"{slug} create domain {name}")
        assert (domdir / name).is_file()

        cp = run_cmd(
            "tdx-domain",
            "info",
            "--domain",
            name,
            *D,
            "--format",
            "json",
            env=vault_env,
        )
        assert_ok(cp, f"{slug} domain info")
    finally:
        run_cmd("tdx-domain", "destroy", "--domain", name, *D, env=vault_env)
        run_cmd("tdx-system", "destroy", "--sys", sysname, *S, env=vault_env)
