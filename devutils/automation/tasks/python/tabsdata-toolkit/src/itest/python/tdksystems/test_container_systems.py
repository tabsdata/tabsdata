"""Integration tests: every container system can be created and exercised
through its tdx lifecycle (create, status, create domain, destroy).

All artefacts land under `target/itest/<test-id>/`. Skips entirely
when Docker isn't available."""
from __future__ import annotations

import platform
from pathlib import Path

import pytest

from tabsdatax.itest.tdksystems import systems


def _container_slugs() -> list[str]:
    return [s for s in systems.all_slugs()
            if systems.get(s).category == "container"]

from conftest import assert_ok, have_docker_running, run_cmd

pytestmark = pytest.mark.integration


pytest.importorskip(
    "docker", reason="Docker SDK not installed",
) if False else None  # we only need the docker daemon; no SDK required.


SQL_MODEL = "courierlogistics"  # ships DDL for mysql/postgres/mssqlserver/oracle/db2


def _ready_timeout(slug: str) -> int:
    """Some images are slow to come up — give them more headroom."""
    return {
        "oracle":   600,
        "mssqlserver": 300,
        "db2":      900,
        "starrocks": 300,
    }.get(slug, 120)


@pytest.mark.parametrize("slug", _container_slugs())
def test_container_system_lifecycle(slug, itest_dir):
    if not have_docker_running():
        pytest.skip("docker daemon not reachable")
    if slug == "starrocks" and platform.machine() == "arm64":
        # The starrocks/allin1-ubuntu BE crashes under qemu-amd64 emulation on
        # Apple Silicon ("beservice FATAL Exited too quickly") — `SHOW BACKENDS`
        # never reports Alive=true, so readiness times out. Known image quirk.
        pytest.skip("starrocks allin1 BE does not run under arm64 emulation")

    sys_desc = itest_dir / f"{slug}.json"
    dom_desc = itest_dir / f"{slug}_dom.json"

    # File-staging systems (clickhouse / starrocks) require a staging root.
    staging = (["--sys-path", str(itest_dir / "staging")]
               if systems.get(slug).staging_root else [])

    cp = run_cmd(
        "tdx-system", "create", "--type", slug,
        "--sys", str(sys_desc), "--name", f"{slug}_sys",
        "--force", "--ready-timeout", str(_ready_timeout(slug)), *staging,
    )
    assert_ok(cp, f"tdx-system create {slug}")
    assert sys_desc.is_file()

    try:
        # status: container should be ready immediately after create returns.
        cp = run_cmd("tdx-system", "status", "--sys", str(sys_desc))
        assert_ok(cp, f"{slug} status")
        assert "ready" in cp.stdout.lower()

        # Create a domain (a database / topic / collection, per backend).
        cp = run_cmd("tdx-domain", "create", "--sys", str(sys_desc),
                     "--domain", str(dom_desc), "--name", "smoke", "--force")
        assert_ok(cp, f"{slug} create domain")
        assert dom_desc.is_file()

        cp = run_cmd("tdx-domain", "status", "--domain", str(dom_desc))
        assert_ok(cp, f"{slug} domain status")

        assert_ok(run_cmd("tdx-domain", "destroy", "--domain", str(dom_desc)),
                  f"{slug} destroy domain")
    finally:
        # Always tear the container down, even if a step above failed.
        run_cmd("tdx-system", "destroy", "--sys", str(sys_desc))


@pytest.mark.parametrize("slug", ["mysql", "postgres", "mssqlserver"])
def test_data_create_load_against_sql_system(slug, itest_dir):
    """Higher-level smoke: create a domain with a model + data in one shot."""
    if not have_docker_running():
        pytest.skip("docker daemon not reachable")

    sys_desc = itest_dir / f"{slug}.json"
    dom_desc = itest_dir / f"{slug}_db.json"

    cp = run_cmd(
        "tdx-system", "create", "--type", slug,
        "--sys", str(sys_desc), "--name", f"{slug}_sys",
        "--force", "--ready-timeout", str(_ready_timeout(slug)),
    )
    assert_ok(cp, f"tdx-system create {slug}")

    try:
        # create-domain installs the model schema (DDL) and loads its data.
        cp = run_cmd("tdx-domain", "create", "--sys", str(sys_desc),
                     "--domain", str(dom_desc), "--name", "smoke",
                     "--model", SQL_MODEL, "--data", "xs", "--force")
        assert_ok(cp, "create domain + model + data")

        cp = run_cmd("tdx-domain", "info", "--domain", str(dom_desc),
                     "--format", "json")
        assert_ok(cp, "domain info")
        assert SQL_MODEL in cp.stdout      # descriptor records the model
    finally:
        run_cmd("tdx-domain", "destroy", "--domain", str(dom_desc))
        run_cmd("tdx-system", "destroy", "--sys", str(sys_desc))
