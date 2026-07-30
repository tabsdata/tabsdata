#
# Copyright 2026 Tabsdata Inc.
#

"""Report a system's readiness."""

from __future__ import annotations

import argparse

from tabsdatax.itest.tdksystems2 import cli, console, ssh
from tabsdatax.itest.tdksystems2.shell import port_open


def add_arguments(parser: argparse.ArgumentParser) -> None:
    cli.add_sys(parser)
    cli.add_sys_dir(parser)


def _tunnel_status(meta: dict[str, str]) -> tuple[bool, str]:
    """A connected system is only as healthy as its tunnel: the backend's own
    probe would look for a local container that lives on the far host."""
    pid = int(meta[ssh.TUNNEL_PID])
    port = meta.get("PORT", "")
    where = f"{meta.get(ssh.SSH_USER) or ''}@{meta.get(ssh.SSH_HOST, '?')}".lstrip("@")
    if not ssh.tunnel_alive(pid):
        return False, f"ssh tunnel to {where} is gone (pid {pid})"
    if not (port.isdigit() and port_open("127.0.0.1", int(port))):
        return False, f"ssh tunnel to {where} is up but localhost:{port} is not"
    return True, f"tunnelled to {where} via localhost:{port}"


def run(ctx: cli.Context) -> int:
    assert ctx.info is not None
    meta = ctx.info.meta
    if ssh.is_tunnelled(meta):
        ok, detail = _tunnel_status(meta)
    else:
        ok, detail = ctx.system.system_status(meta)
    (console.good if ok else console.bad)(f"{ctx.name}: {detail}")
    return 0 if ok else 1
