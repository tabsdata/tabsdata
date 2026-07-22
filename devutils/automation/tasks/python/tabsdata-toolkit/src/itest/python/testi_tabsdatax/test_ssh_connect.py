#
# Copyright 2026 Tabsdata Inc.
#

"""Integration: `tdx-system connect` over SSH, tunnelling to localhost.

Stands up a real mysql container (the "server"), then connects to it through an
SSH tunnel to localhost, provisions a domain *through the tunnel* (TCP admin, no
docker exec), and tears everything down — destroying the local handle stops the
tunnel and leaves the container. Skips unless Docker and passwordless ssh to
localhost are both available."""

from __future__ import annotations

import getpass
import subprocess

import pytest
from conftest import assert_ok, have_docker_running, run_cmd

pytestmark = pytest.mark.integration


def _ssh_localhost_ok() -> bool:
    return (
        subprocess.run(
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=4",
                "-o",
                "StrictHostKeyChecking=accept-new",
                "localhost",
                "true",
            ],
            capture_output=True,
        ).returncode
        == 0
    )


def test_connect_tunnel_lifecycle_over_localhost(itest_dir):
    if not have_docker_running():
        pytest.skip("docker daemon not reachable")
    if not _ssh_localhost_ok():
        pytest.skip("passwordless ssh to localhost not available")

    # Separate stores for the "remote" server and the local (connected) handle.
    remote_dir, local_dir, dom_dir = (
        itest_dir / "remote-systems",
        itest_dir / "systems",
        itest_dir / "domains",
    )
    RS = ("--sys-dir", str(remote_dir))
    LS = ("--sys-dir", str(local_dir))
    D = ("--domain-dir", str(dom_dir))
    remote_path = remote_dir / "shop"  # scp-style --remote-sys needs the path
    local_path, dom_path = local_dir / "local_mysql", dom_dir / "smoke"
    user = getpass.getuser()

    # 1. The "server": a real mysql container + its system descriptor.
    assert_ok(
        run_cmd(
            "tdx-system",
            "create",
            "--type",
            "mysql",
            "--sys",
            "shop",
            *RS,
            "--force",
            "--ready-timeout",
            "120",
        ),
        "create remote mysql",
    )
    try:
        # 2. Connect over ssh-to-localhost: opens the tunnel + writes the handle.
        #    The remote was created under $TD_BACKENDS_CRYPTO_KEY; --remote-secret-env
        #    (mandatory) names the env var to decrypt it with.
        assert_ok(
            run_cmd(
                "tdx-system",
                "connect",
                "--remote-sys",
                f"{user}@localhost:{remote_path}",
                "--sys",
                "local_mysql",
                *LS,
                "--remote-secret-env",
                "TD_BACKENDS_CRYPTO_KEY",
                "--force",
            ),
            "tdx-system connect",
        )
        assert local_path.is_file()

        # status of the connected handle checks the tunnel (not a container).
        cp = run_cmd("tdx-system", "status", "--sys", "local_mysql", *LS)
        assert_ok(cp, "connected status")
        assert "ready" in cp.stdout.lower()

        # 3. Provision a domain THROUGH THE TUNNEL (TCP admin, no local docker).
        assert_ok(
            run_cmd(
                "tdx-domain",
                "create",
                "--sys",
                "local_mysql",
                *LS,
                "--domain",
                "smoke",
                *D,
                "--force",
            ),
            "create domain over tunnel",
        )
        assert dom_path.is_file()
        assert_ok(
            run_cmd("tdx-domain", "status", "--domain", "smoke", *D),
            "domain status over tunnel",
        )
        assert_ok(
            run_cmd("tdx-domain", "destroy", "--domain", "smoke", *D),
            "destroy domain over tunnel",
        )

        # 4. Destroying the local handle stops the tunnel (container untouched).
        assert_ok(
            run_cmd("tdx-system", "destroy", "--sys", "local_mysql", *LS),
            "destroy connected handle",
        )
        assert not local_path.exists()
    finally:
        # Best-effort: stop a leaked tunnel, then remove the container.
        run_cmd("tdx-system", "destroy", "--sys", "local_mysql", *LS)
        run_cmd("tdx-system", "destroy", "--sys", "shop", *RS)
