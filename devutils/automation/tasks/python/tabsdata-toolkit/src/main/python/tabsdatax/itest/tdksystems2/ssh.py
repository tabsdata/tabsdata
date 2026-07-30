#
# Copyright 2026 Tabsdata Inc.
#

"""SSH helpers for ``tdx-system connect``: fetch a remote (encrypted) descriptor
over SSH and run a detached, multiplatform local-forward tunnel.

The tunnel is a plain ``ssh -N -L`` child process, detached so it outlives the
`connect` command and tracked by PID in the connected system descriptor. We
shell out to the OS ``ssh`` client (not a Python SSH library) so it honours
``~/.ssh/config``, keys and ssh-agent, and we deliberately avoid ControlMaster
and ``ssh -f`` — neither works on Windows OpenSSH. A detached ``Popen`` + PID is
the portable baseline: ``start_new_session`` (POSIX) / ``CREATE_NEW_PROCESS_GROUP``
(Windows) to detach, ``killpg`` / ``taskkill /T`` to tear down.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from dataclasses import dataclass

from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.shell import port_open

# Descriptor meta keys a connected (tunnelled) system carries. TUNNEL_PID is the
# marker: its presence means the system is reached through a tunnel this machine
# owns, so status probes the tunnel and destroy tears it down instead of going to
# a local Docker daemon that knows nothing about it.
TUNNEL_PID = "SSH_TUNNEL_PID"
SSH_HOST = "SSH_HOST"
SSH_USER = "SSH_USER"
SSH_PORT = "SSH_PORT"
SSH_TARGET_PORT = "SSH_TARGET_PORT"


def is_tunnelled(meta: dict[str, str]) -> bool:
    return bool(meta.get(TUNNEL_PID))


@dataclass
class SshEndpoint:
    """An SSH endpoint reused for the file fetch and the tunnel."""

    host: str
    user: str | None = None
    port: int = 22
    identity: str | None = None

    @property
    def target(self) -> str:
        return f"{self.user}@{self.host}" if self.user else self.host

    def base_args(self) -> list[str]:
        args = ["-p", str(self.port)]
        if self.identity:
            args += ["-i", self.identity]
        return args


def parse_remote_spec(spec: str) -> tuple[SshEndpoint, str]:
    """Parse an scp-style ``[user@]host:path`` into (endpoint, remote_path)."""
    host_part, sep, path = spec.partition(":")
    if not sep or not host_part or not path:
        raise TdxError(f"remote must be [user@]HOST:PATH (got: {spec!r})")
    user = None
    if "@" in host_part:
        user, host_part = host_part.split("@", 1)
        # An empty user is a typo, not "no user" — say so rather than quietly
        # connecting as whoever ssh would default to.
        if not user:
            raise TdxError(f"remote must be [user@]HOST:PATH (got: {spec!r})")
    if not host_part:
        raise TdxError(f"remote must be [user@]HOST:PATH (got: {spec!r})")
    return SshEndpoint(host=host_part, user=user), path


def fetch_remote_file(ep: SshEndpoint, remote_path: str) -> str:
    """Stream a remote text file over SSH (``ssh … -- cat -- PATH``). The
    descriptor is PEM/ASCII, so `cat` is safe and no `scp` protocol is needed."""
    cp = subprocess.run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            *ep.base_args(),
            ep.target,
            "--",
            "cat",
            "--",
            remote_path,
        ],
        capture_output=True,
        encoding="utf-8",
    )
    if cp.returncode != 0:
        raise TdxError(
            f"could not fetch {remote_path} from {ep.target}: "
            f"{cp.stderr.strip() or cp.stdout.strip() or 'ssh failed'}"
        )
    return cp.stdout


def _popen_detached(args: list[str], log_path: str) -> subprocess.Popen:
    logf = open(log_path, "ab")
    if os.name == "nt":
        flags = (
            subprocess.CREATE_NO_WINDOW  # type: ignore[attr-defined]
            | subprocess.CREATE_NEW_PROCESS_GROUP
        )  # type: ignore[attr-defined]
        return subprocess.Popen(
            args,
            stdin=subprocess.DEVNULL,
            stdout=logf,
            stderr=logf,
            creationflags=flags,
        )
    # setsid: own session/process group, detached from the controlling terminal,
    # so it survives `connect` exiting and can be signalled as a group.
    return subprocess.Popen(
        args, stdin=subprocess.DEVNULL, stdout=logf, stderr=logf, start_new_session=True
    )


def open_tunnel(
    ep: SshEndpoint,
    local_port: int,
    target_port: int,
    log_path: str,
    *,
    ready_timeout: float = 20.0,
) -> int:
    """Start a detached ``ssh -N -L 127.0.0.1:local -> 127.0.0.1:target`` tunnel
    and return its PID once the local port accepts connections.

    ``ExitOnForwardFailure=yes`` makes ssh exit if the forward can't bind, so a
    failure surfaces as the process dying (and the readiness probe failing)
    rather than a silent half-open tunnel."""
    args = [
        "ssh",
        "-N",
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        "BatchMode=yes",
        "-o",
        "ServerAliveInterval=30",
        "-o",
        "ServerAliveCountMax=3",
        "-L",
        f"127.0.0.1:{local_port}:127.0.0.1:{target_port}",
        *ep.base_args(),
        ep.target,
    ]
    proc = _popen_detached(args, log_path)
    deadline = time.monotonic() + ready_timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise TdxError(
                f"ssh tunnel to {ep.target} exited (rc={proc.returncode}): "
                f"{_read_tail(log_path)}"
            )
        if port_open("127.0.0.1", local_port, timeout=1.0):
            return proc.pid
        time.sleep(0.3)
    close_tunnel(proc.pid)
    raise TdxError(
        f"ssh tunnel to {ep.target} did not open 127.0.0.1:{local_port} within "
        f"{ready_timeout:.0f}s: {_read_tail(log_path)}"
    )


def tunnel_alive(pid: int) -> bool:
    if os.name == "nt":
        cp = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}"], capture_output=True, encoding="utf-8"
        )
        return str(pid) in cp.stdout
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def close_tunnel(pid: int) -> None:
    """Stop the tunnel process. POSIX signals the whole process group (the ssh
    child was started in its own session); Windows uses ``taskkill /T`` to take
    the tree down. Best-effort — a stale/absent PID is not an error."""
    if os.name == "nt":
        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True)
        return
    try:
        os.killpg(os.getpgid(pid), signal.SIGTERM)
    except OSError:
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            pass


def _read_tail(path: str, n: int = 400) -> str:
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return f.read()[-n:].strip()
    except OSError:
        return ""
