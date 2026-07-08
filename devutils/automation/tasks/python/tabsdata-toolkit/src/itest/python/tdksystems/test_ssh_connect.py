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
    return subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=4",
         "-o", "StrictHostKeyChecking=accept-new", "localhost", "true"],
        capture_output=True,
    ).returncode == 0


def test_connect_tunnel_lifecycle_over_localhost(itest_dir):
    if not have_docker_running():
        pytest.skip("docker daemon not reachable")
    if not _ssh_localhost_ok():
        pytest.skip("passwordless ssh to localhost not available")

    remote_sys = itest_dir / "remote_mysql.tdx"
    local_sys = itest_dir / "local_mysql.tdx"
    dom = itest_dir / "dom.tdx"
    user = getpass.getuser()

    # 1. The "server": a real mysql container + its system descriptor.
    assert_ok(run_cmd("tdx-system", "create", "--type", "mysql",
                      "--sys", str(remote_sys), "--name", "shop", "--force",
                      "--ready-timeout", "120"), "create remote mysql")
    try:
        # 2. Connect over ssh-to-localhost: opens the tunnel + writes the handle.
        assert_ok(run_cmd("tdx-system", "connect",
                          "--remote-sys", f"{user}@localhost:{remote_sys}",
                          "--sys", str(local_sys), "--force"),
                  "tdx-system connect")
        assert local_sys.is_file()

        # status of the connected handle checks the tunnel (not a container).
        cp = run_cmd("tdx-system", "status", "--sys", str(local_sys))
        assert_ok(cp, "connected status")
        assert "ready" in cp.stdout.lower()

        # 3. Provision a domain THROUGH THE TUNNEL (TCP admin, no local docker).
        assert_ok(run_cmd("tdx-domain", "create", "--sys", str(local_sys),
                          "--domain", str(dom), "--name", "smoke", "--force"),
                  "create domain over tunnel")
        assert dom.is_file()
        assert_ok(run_cmd("tdx-domain", "status", "--domain", str(dom)),
                  "domain status over tunnel")
        assert_ok(run_cmd("tdx-domain", "destroy", "--domain", str(dom)),
                  "destroy domain over tunnel")

        # 4. Destroying the local handle stops the tunnel (container untouched).
        assert_ok(run_cmd("tdx-system", "destroy", "--sys", str(local_sys)),
                  "destroy connected handle")
        assert not local_sys.exists()
    finally:
        # Best-effort: stop a leaked tunnel, then remove the container.
        run_cmd("tdx-system", "destroy", "--sys", str(local_sys))
        run_cmd("tdx-system", "destroy", "--sys", str(remote_sys))
