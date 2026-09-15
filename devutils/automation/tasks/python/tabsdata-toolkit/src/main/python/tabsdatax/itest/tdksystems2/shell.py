#
# Copyright 2026 Tabsdata Inc.
#

"""Docker / subprocess / env helpers, plus the padded ``ArgumentParser``.

Wraps ``docker`` for the container lifecycle, generates passwords and free
ports, and reads env vars with a clear error when missing. ``ArgumentParser``
wraps help / usage / error output with blank lines and routes it to the real
terminal when a verb is running under the console log split.
"""

from __future__ import annotations

import argparse
import os
import secrets
import socket
import string
import subprocess

from tabsdatax.itest.tdksystems2.errors import TdxError


class ArgumentParser(argparse.ArgumentParser):
    """argparse parser that pads help/usage/error with blank lines and sends
    them to the real terminal (not the detail log) when a verb is logging."""

    def print_help(self, file=None):
        from tabsdatax.itest.tdksystems2 import console

        file = file or console.stream()
        print(file=file)
        super().print_help(file)
        print(file=file)

    def print_usage(self, file=None):
        from tabsdatax.itest.tdksystems2 import console

        file = file or console.stream()
        print(file=file)
        super().print_usage(file)

    def error(self, message: str):
        from tabsdatax.itest.tdksystems2 import console

        err = console.err_stream()
        self.print_usage(err)
        err.write(f"{self.prog}: error: {message}\n\n")
        err.flush()
        raise SystemExit(2)


# The punctuation a generated password may use: the URL-unreserved set, and
# nothing else. A password reaches a server three ways and each rules something
# out. It is interpolated into a quoted SQL literal (`IDENTIFIED BY '...'`), so
# no quote and no backslash. It is executed through a DB-API driver's
# `exec_driver_sql`, which reads `%` as a placeholder -- the same reason the
# postgres DDL avoids it. And it is put in a SQLAlchemy URL that only the
# managed backends percent-encode, so nothing URL-reserved (`@ : / ? # &`).
# These four survive all three untouched.
SAFE_SYMBOLS = "-._~"


def gen_password(length: int = 24) -> str:
    """`length` chars: 4 upper + 4 lower + 4 digits + 4 symbols + remainder
    alphanumeric, so it satisfies every database's complexity rule at once.

    The symbols are what makes that true rather than nearly true: Cloud SQL
    rejects a password with no non-alphanumeric character outright
    (``INVALID_PASSWORD``), and an alphanumeric generator only ever passed
    everywhere else by luck of what was not being enforced.
    """
    upper = "".join(secrets.choice(string.ascii_uppercase) for _ in range(4))
    lower = "".join(secrets.choice(string.ascii_lowercase) for _ in range(4))
    digit = "".join(secrets.choice(string.digits) for _ in range(4))
    symbol = "".join(secrets.choice(SAFE_SYMBOLS) for _ in range(4))
    rest = "".join(
        secrets.choice(string.ascii_letters + string.digits)
        for _ in range(max(length - 16, 0))
    )
    return upper + lower + digit + symbol + rest


def find_free_port() -> int:
    s = socket.socket()
    try:
        s.bind(("", 0))
        return s.getsockname()[1]
    finally:
        s.close()


def find_free_ports(count: int) -> list[int]:
    """`count` *distinct* free ports, found by holding every candidate socket open
    at once so the OS hands out different ports, then releasing them. The ports
    are free (not reserved) when returned; a caller that fans a disjoint slice out
    to each of several parallel workers keeps those workers from racing one
    another for the same free port."""
    socks: list[socket.socket] = []
    try:
        for _ in range(max(0, count)):
            s = socket.socket()
            s.bind(("", 0))
            socks.append(s)
        return [s.getsockname()[1] for s in socks]
    finally:
        for s in socks:
            s.close()


def port_open(host: str, port: int, timeout: float = 3.0) -> bool:
    """True if a TCP connection to host:port succeeds within `timeout` seconds."""
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
        raise TdxError("docker not found in PATH or daemon not running")


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
    """`running`/`exited`/`created`/etc., or None if the container is absent."""
    cp = subprocess.run(
        ["docker", "container", "inspect", "-f", "{{.State.Status}}", name],
        capture_output=True,
        encoding="utf-8",
    )
    if cp.returncode != 0:
        return None
    return cp.stdout.strip() or None


def container_id(name: str) -> str | None:
    """Full docker id of container `name`, or None if absent. Raises TdxError if
    docker itself is unavailable (the caller can't tell staleness from that)."""
    try:
        cp = subprocess.run(
            ["docker", "container", "inspect", "-f", "{{.Id}}", name],
            capture_output=True,
            encoding="utf-8",
        )
    except FileNotFoundError as exc:
        raise TdxError("docker is not available") from exc
    if cp.returncode != 0:
        if "no such" in (cp.stderr or "").lower():
            return None
        raise TdxError(f"docker inspect failed: {cp.stderr.strip()}")
    return cp.stdout.strip() or None


def docker_pull(image: str, platform: str = "") -> None:
    """Pull `image`, streaming docker's own download progress to stdout/stderr."""
    args = ["docker", "pull"]
    if platform:
        args += ["--platform", platform]
    args.append(image)
    if subprocess.run(args).returncode != 0:
        raise TdxError(f"docker pull failed for image {image}")


def docker_run(args: list[str]) -> str:
    cp = subprocess.run(["docker", "run"] + args, capture_output=True, encoding="utf-8")
    if cp.returncode != 0:
        raise TdxError(f"docker run failed: {cp.stderr.strip() or cp.stdout.strip()}")
    cid = cp.stdout.strip()
    if not cid:
        raise TdxError("docker run produced no container id")
    return cid


def docker_rm(name: str, force: bool = False) -> None:
    args = ["docker", "rm"]
    if force:
        args.append("-f")
    args.append(name)
    cp = subprocess.run(args, capture_output=True, encoding="utf-8")
    if cp.returncode != 0:
        detail = cp.stderr.strip() or cp.stdout.strip()
        raise TdxError(f"could not remove container {name}: {detail}")


def docker_start(name: str) -> None:
    cp = subprocess.run(
        ["docker", "start", name], capture_output=True, encoding="utf-8"
    )
    if cp.returncode != 0:
        detail = cp.stderr.strip() or cp.stdout.strip()
        raise TdxError(f"could not start container {name}: {detail}")


def docker_stop(name: str) -> None:
    cp = subprocess.run(["docker", "stop", name], capture_output=True, encoding="utf-8")
    if cp.returncode != 0:
        detail = cp.stderr.strip() or cp.stdout.strip()
        raise TdxError(f"could not stop container {name}: {detail}")


def docker_restart(name: str) -> None:
    cp = subprocess.run(
        ["docker", "restart", name], capture_output=True, encoding="utf-8"
    )
    if cp.returncode != 0:
        detail = cp.stderr.strip() or cp.stdout.strip()
        raise TdxError(f"could not restart container {name}: {detail}")


def docker_exec(
    container: str,
    *cmd: str,
    input: str | None = None,
    user: str | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    """Run a command in `container`, capturing stdout/stderr (and feeding
    `input` on stdin). Used by readiness probes / post-start hooks that need the
    command's output, not just its exit code."""
    args = ["docker", "exec", "-i"]
    if user is not None:
        args += ["-u", user]
    if env:
        for k, v in env.items():
            args += ["-e", f"{k}={v}"]
    args += [container, *cmd]
    return subprocess.run(args, input=input, capture_output=True, encoding="utf-8")


def docker_exec_check(container: str, *cmd: str) -> bool:
    """Run a check command in `container`; return True iff exit code 0."""
    return (
        subprocess.run(
            ["docker", "exec", container] + list(cmd),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode
        == 0
    )


def validate_name(name: str, label: str = "name") -> None:
    if not name:
        raise TdxError(f"{label} is empty")
    for ch in name:
        if not (ch.isalnum() or ch == "_"):
            raise TdxError(f"{label} must be alphanumeric/underscore (got: {name})")


def http_ok(
    url: str,
    timeout: float = 5.0,
    *,
    user: str | None = None,
    password: str | None = None,
) -> bool:
    """True if a GET to `url` returns a 2xx within `timeout`s (an HTTP readiness
    probe, e.g. Druid's /status). With `user`, sends HTTP basic-auth."""
    import base64
    import urllib.request

    req = urllib.request.Request(url)  # noqa: S310
    if user is not None:
        token = base64.b64encode(f"{user}:{password or ''}".encode()).decode()
        req.add_header("Authorization", f"Basic {token}")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
            return 200 <= resp.status < 300
    except Exception:  # noqa: BLE001 - any failure means "not ready"
        return False


def docker_compose(
    project: str,
    compose_file: str,
    *args: str,
    capture: bool = False,
    env: dict[str, str] | None = None,
) -> str:
    """Run ``docker compose -p <project> -f <file> <args...>``. Streams output by
    default; ``capture=True`` returns stdout. ``env`` adds variables the compose
    file interpolates (e.g. the planned host ports)."""
    cmd = ["docker", "compose", "-p", project, "-f", compose_file, *args]
    run_env = {**os.environ, **env} if env else None
    verb = args[0] if args else ""
    if not capture:
        if subprocess.run(cmd, env=run_env).returncode != 0:
            raise TdxError(f"docker compose {verb} failed for project {project}")
        return ""
    cp = subprocess.run(cmd, capture_output=True, encoding="utf-8", env=run_env)
    if cp.returncode != 0:
        detail = (cp.stderr or cp.stdout or "").strip()
        raise TdxError(f"docker compose {verb} failed for project {project}: {detail}")
    return cp.stdout.strip()


def compose_container_ids(project: str, compose_file: str) -> list[str]:
    """Ids of every container of the compose project (running or stopped) — an
    existence check before ``up``."""
    out = docker_compose(project, compose_file, "ps", "-a", "-q", capture=True)
    return [line for line in out.splitlines() if line.strip()]
