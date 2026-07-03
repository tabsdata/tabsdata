#
# Copyright 2026 Tabs Data Inc.
#

import json
import os
import socket
import time
from pathlib import Path

import psutil
import rich_click as click
from filelock import FileLock, Timeout

LOCALHOST = "127.0.0.1"


def claims_file(lock_file: Path) -> Path:
    return Path(f"{lock_file}.json")


def load_claims(registry_file: Path) -> list[dict]:
    if not registry_file.exists():
        return []
    try:
        with open(registry_file, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (json.JSONDecodeError, OSError):
        return []
    if not isinstance(data, list):
        return []
    return data


def save_claims(registry_file: Path, claims: list[dict]):
    with open(registry_file, "w", encoding="utf-8") as handle:
        json.dump(claims, handle, indent=2)


def port_free(value: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        if os.name == "nt":
            exclusive = getattr(socket, "SO_EXCLUSIVEADDRUSE", None)
            if exclusive is not None:
                probe.setsockopt(socket.SOL_SOCKET, exclusive, 1)
        try:
            probe.bind((LOCALHOST, value))
        except OSError:
            return False
    return True


def window_free(base: int, count: int) -> bool:
    return all(port_free(base + offset) for offset in range(count))


def prune_claims(claims: list[dict], count: int) -> list[dict]:
    surviving = []
    for claim in claims:
        if not isinstance(claim, dict):
            continue
        base = claim.get("base")
        process = claim.get("pid")
        if not isinstance(base, int):
            continue
        if isinstance(process, int) and psutil.pid_exists(process):
            surviving.append(claim)
        elif not window_free(base, count):
            surviving.append(claim)
    return surviving


def claimed_ports(claims: list[dict], count: int) -> set[int]:
    occupied: set[int] = set()
    for claim in claims:
        base = claim.get("base")
        if isinstance(base, int):
            occupied.update(range(base, base + count))
    return occupied


def pick_base(
    claims: list[dict],
    count: int,
    floor: int,
    ceiling: int,
    stride: int,
):
    occupied = claimed_ports(claims, count)
    candidate = floor
    while candidate + count - 1 <= ceiling:
        window = range(candidate, candidate + count)
        if occupied.isdisjoint(window) and window_free(candidate, count):
            return candidate
        candidate += stride
    return None


@click.command(name="port")
@click.argument("lock_file", type=click.Path(path_type=Path))
@click.option("--count", type=int, default=11)
@click.option("--floor", type=int, default=20000)
@click.option("--ceiling", type=int, default=60000)
@click.option("--stride", type=int, default=0)
@click.option("--timeout", type=float, default=30.0)
def port(
    lock_file: Path,
    count: int,
    floor: int,
    ceiling: int,
    stride: int,
    timeout: float,
):
    """Pick a base port whose window of 'count' ports is free, and claim it.

    The claim is recorded under a file lock so that concurrent itest runs on
    the same machine never select overlapping windows; the residual gap before
    tdkserver binds is covered by the post-start status verification.
    """
    if stride <= 0:
        stride = count
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    registry_file = claims_file(lock_file)

    lock = FileLock(str(lock_file), timeout=timeout)
    try:
        with lock:
            claims = prune_claims(load_claims(registry_file), count)
            base = pick_base(claims, count, floor, ceiling, stride)
            if base is None:
                raise click.ClickException(
                    f"No free port window of {count} ports found in "
                    f"[{floor}, {ceiling}]."
                )
            claims.append({
                "base": base,
                "count": count,
                "pid": os.getpid(),
                "stamp": time.time(),
            })
            save_claims(registry_file, claims)
    except Timeout:
        raise click.ClickException(
            f"Timed out after {timeout}s waiting for the port claims lock "
            f"'{lock_file}'."
        )

    click.echo(base)
