#
# Copyright 2026 Tabsdata Inc.
#

"""Reach a container system running on another machine, over an SSH tunnel.

Fetches that machine's (encrypted) system descriptor over SSH, opens a detached
``ssh -L`` tunnel to the container's published port, and writes a *local*
descriptor whose HOST/PORT point at the near end of it. Everything downstream —
``tdx-domain create``, the data plane, a connector under test — then works
against 127.0.0.1 with no local Docker daemon and no knowledge that the system is
remote.

Two secrets are in play: the remote descriptor is encrypted under the server's
(named by ``--remote-secret-env``), and the local one is written under this
machine's ``$TD_BACKENDS_CRYPTO_KEY``. The server side runs nothing
tdx-specific — it just has to be reachable by ssh and hold a descriptor file.

Container systems only. An object store, a warehouse or a credential-only
service has no TCP endpoint on the far host to forward, and a file system's
staging root is a server-side path a tunnel cannot reach.
"""

from __future__ import annotations

import argparse
import os

from tabsdatax.itest.tdksystems2 import (
    cli,
    console,
    constants,
    crypto,
    descriptor,
    ssh,
    store,
    systems,
)
from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.shell import find_free_port

# Copied from the remote descriptor onto the local one; HOST and PORT are
# replaced by the tunnel's near end, and anything docker-local is dropped.
_CARRIED = ("USER", "PASSWORD", "CONTAINER", "IMAGE", "SYSTEM_KIND")


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        constants.OPT_REMOTE_SYS,
        required=True,
        metavar="[user@]HOST:PATH",
        help="ssh endpoint and path to the remote (encrypted) system descriptor",
    )
    parser.add_argument(
        constants.OPT_REMOTE_SECRET_ENV,
        required=True,
        metavar="VAR",
        help="env var holding the secret that decrypts the REMOTE descriptor "
        f"(the local one is written under ${constants.ENV_CRYPTO_KEY})",
    )
    parser.add_argument(
        constants.OPT_SSH_PORT, type=int, default=22, help="ssh port (default: 22)"
    )
    parser.add_argument(
        constants.OPT_IDENTITY,
        default=None,
        metavar="KEYFILE",
        help="ssh identity file (ssh -i)",
    )
    cli.add_sys(parser, multi=False)
    cli.add_sys_dir(parser)
    cli.add_force(parser)
    cli.add_latch(parser)


def _remote_secret(var: str) -> str:
    secret = os.environ.get(var, "")
    if not secret:
        raise TdxError(f"${var} is not set — it decrypts the remote descriptor")
    return secret


def run(ctx: cli.Context) -> int:
    a = ctx.args
    name = a.sys
    descriptor.validate_name(name)
    path = store.system_path(name, a.sys_dir)
    if path.exists() and not a.force:
        raise TdxError(
            f"system '{name}' already exists at {path} "
            f"(use {constants.OPT_FORCE} to replace it)"
        )

    endpoint, remote_path = ssh.parse_remote_spec(a.remote_sys)
    endpoint.port = a.ssh_port
    endpoint.identity = a.identity

    text = ssh.fetch_remote_file(endpoint, remote_path)
    with crypto.using_secret(
        _remote_secret(a.remote_secret_env), what=f"${a.remote_secret_env}"
    ):
        remote = descriptor.loads(text)
    if not isinstance(remote, descriptor.SystemInfo):
        raise TdxError(f"{a.remote_sys} is a {remote.role} descriptor, not a system")

    slug = remote.system
    backend = systems.get_class(slug)
    if not getattr(backend, "is_container", False):
        raise TdxError(
            f"'{slug}' runs no container, so it has no port on {endpoint.host} to "
            f"forward — {constants.SYS_TOOL} {constants.V_CONNECT} is for container "
            f"systems"
        )
    target_port = remote.meta.get("PORT", "")
    if not target_port.isdigit():
        raise TdxError(
            f"the remote descriptor has no numeric PORT to tunnel to "
            f"(got {target_port!r})"
        )

    local_port = find_free_port()
    log_path = f"{path}.ssh.log"
    console.out(
        f">>> opening ssh tunnel to {endpoint.target}: 127.0.0.1:{local_port} -> "
        f"127.0.0.1:{target_port} (container {remote.meta.get('CONTAINER', '?')})"
    )
    pid = ssh.open_tunnel(endpoint, local_port, int(target_port), log_path)

    meta = {k: remote.meta[k] for k in _CARRIED if k in remote.meta}
    meta.update({
        "HOST": "127.0.0.1",
        "PORT": str(local_port),
        ssh.SSH_HOST: endpoint.host,
        ssh.SSH_USER: endpoint.user or "",
        ssh.SSH_PORT: str(endpoint.port),
        ssh.SSH_TARGET_PORT: target_port,
        ssh.TUNNEL_PID: str(pid),
    })
    try:
        descriptor.write_system(path, slug, name, meta)
    except BaseException:
        # No descriptor means nothing would ever tear this tunnel down.
        ssh.close_tunnel(pid)
        raise
    console.good(
        f"connected '{name}' -> {endpoint.target} (127.0.0.1:{local_port}); "
        f"destroy it to close the tunnel"
    )
    return 0
