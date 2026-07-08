"""Small helpers shared across container + cloud subclasses.

Wraps subprocess for docker, generates passwords / free ports, and reads
required env vars (with a clear error if missing). Also defines the
`ArgumentParser` subclass used everywhere so help/usage/error output
gets a leading and trailing blank line.
"""

from __future__ import annotations

import argparse
import os
import secrets
import socket
import string
import subprocess
import sys

from .errors import DatagenError


class ArgumentParser(argparse.ArgumentParser):
    """argparse parser that wraps every help/usage/error block with a
    leading and trailing blank line, consistent with the rest of the
    CLI's output style."""

    def print_help(self, file=None):
        file = file or sys.stdout
        print(file=file)
        super().print_help(file)
        print(file=file)

    def print_usage(self, file=None):
        # Used both directly and indirectly by error(); we add the
        # leading blank here and the trailing blank is added by error()
        # (after the error message) or by print_help() (after the help
        # text, when print_usage is part of the help layout).
        file = file or sys.stdout
        print(file=file)
        super().print_usage(file)

    def error(self, message: str):
        # Default chain: print_usage(stderr) → exit(2, "prog: error: msg\n").
        # Trail an extra newline so a blank line appears below the error.
        self.print_usage(sys.stderr)
        self.exit(2, f"{self.prog}: error: {message}\n\n")


def gen_password(length: int = 24) -> str:
    """24 chars: 4 upper + 4 lower + 4 digits + remainder alphanumeric.
    Satisfies SQL Server 3-of-4 complexity (upper, lower, digit)."""
    upper = "".join(secrets.choice(string.ascii_uppercase) for _ in range(4))
    lower = "".join(secrets.choice(string.ascii_lowercase) for _ in range(4))
    digit = "".join(secrets.choice(string.digits) for _ in range(4))
    rest_len = max(length - 12, 0)
    rest = "".join(
        secrets.choice(string.ascii_letters + string.digits) for _ in range(rest_len)
    )
    return upper + lower + digit + rest


def find_free_port() -> int:
    s = socket.socket()
    try:
        s.bind(("", 0))
        return s.getsockname()[1]
    finally:
        s.close()


def port_open(host: str, port: int, timeout: float = 3.0) -> bool:
    """True if a TCP connection to host:port succeeds within `timeout` seconds.
    A transport-only reachability probe (no auth) — used by domain status so it
    works over a network connection, without a local Docker daemon."""
    s = socket.socket()
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        return True
    except OSError:
        return False
    finally:
        s.close()


def require_docker() -> None:
    if (
        subprocess.run(
            ["docker", "info"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        ).returncode
        != 0
    ):
        raise DatagenError("docker not found in PATH or daemon not running")


def container_exists(name: str) -> bool:
    return (
        subprocess.run(
            ["docker", "container", "inspect", name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode
        == 0
    )


def container_state(name: str) -> str | None:
    """`running`/`exited`/`created`/etc., or None if container doesn't exist."""
    cp = subprocess.run(
        ["docker", "container", "inspect", "-f", "{{.State.Status}}", name],
        capture_output=True,
        text=True,
    )
    if cp.returncode != 0:
        return None
    return cp.stdout.strip() or None


def docker_run(args: list[str]) -> str:
    cp = subprocess.run(["docker", "run"] + args, capture_output=True, text=True)
    if cp.returncode != 0:
        raise DatagenError(
            f"docker run failed: {cp.stderr.strip() or cp.stdout.strip()}"
        )
    cid = cp.stdout.strip()
    if not cid:
        raise DatagenError("docker run produced no container id")
    return cid


def docker_rm(name: str, force: bool = False) -> None:
    args = ["docker", "rm"]
    if force:
        args.append("-f")
    args.append(name)
    cp = subprocess.run(args, capture_output=True, text=True)
    if cp.returncode != 0:
        detail = cp.stderr.strip() or cp.stdout.strip()
        raise DatagenError(f"could not remove container {name}: {detail}")


def docker_start(name: str) -> None:
    cp = subprocess.run(["docker", "start", name], capture_output=True, text=True)
    if cp.returncode != 0:
        detail = cp.stderr.strip() or cp.stdout.strip()
        raise DatagenError(f"could not start container {name}: {detail}")


def docker_stop(name: str) -> None:
    cp = subprocess.run(["docker", "stop", name], capture_output=True, text=True)
    if cp.returncode != 0:
        raise DatagenError(
            f"could not stop container {name}: {cp.stderr.strip() or cp.stdout.strip()}"
        )


def docker_exec(
    container: str,
    *cmd: str,
    input: str | None = None,
    user: str | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    args = ["docker", "exec", "-i"]
    if user is not None:
        args += ["-u", user]
    if env:
        for k, v in env.items():
            args += ["-e", f"{k}={v}"]
    args += [container] + list(cmd)
    return subprocess.run(args, input=input, capture_output=True, text=True)


def docker_exec_check(container: str, *cmd: str) -> bool:
    """Run a check command; return True iff exit code 0."""
    return (
        subprocess.run(
            ["docker", "exec", container] + list(cmd),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode
        == 0
    )


def env_required(name: str) -> str:
    v = os.environ.get(name, "")
    if not v:
        raise DatagenError(f"required env var {name} is not set")
    return v


def env_optional(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def validate_name(name: str, label: str = "name") -> None:
    if not name:
        raise DatagenError(f"{label} is empty")
    for ch in name:
        if not (ch.isalnum() or ch == "_"):
            raise DatagenError(f"{label} must be alphanumeric/underscore (got: {name})")
