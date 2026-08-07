#
# Copyright 2026 Tabsdata Inc.
#

"""System resource reporting for the inspect commands.

Reports what the container actually got, as opposed to what the node
offers. The distinction matters: `os.cpu_count` and `/proc/meminfo`
describe the node, while the cgroup limits describe the share this
container is allowed to use, and a pod is throttled or OOM-killed
against the latter. Both are reported side by side so a sizing mistake
is visible rather than inferred.

Everything is read from `/proc` and `/sys`, so no elevated capability
or service account token is needed. Targets Linux only, which is what
the pods run.

Reporting is diagnostic and must never break the command that calls it.
Every probe is captured on its own, so one unreadable path yields a
single `<error: ...>` value rather than losing the rest of the report,
and the report is buffered and printed as a single block once all
gathering is done, so it is not interleaved with log output.
"""

from __future__ import annotations

import os
import platform
import shutil
import socket
import traceback

CGROUP_ROOT = "/sys/fs/cgroup"

CGROUP_V2_MARKER = os.path.join(CGROUP_ROOT, "cgroup.controllers")
CGROUP_V2_CPU_MAX = os.path.join(CGROUP_ROOT, "cpu.max")
CGROUP_V2_CPU_STAT = os.path.join(CGROUP_ROOT, "cpu.stat")
CGROUP_V2_MEMORY_MAX = os.path.join(CGROUP_ROOT, "memory.max")
CGROUP_V2_MEMORY_HIGH = os.path.join(CGROUP_ROOT, "memory.high")
CGROUP_V2_MEMORY_CURRENT = os.path.join(CGROUP_ROOT, "memory.current")

CGROUP_V1_CPU_QUOTA = os.path.join(CGROUP_ROOT, "cpu", "cpu.cfs_quota_us")
CGROUP_V1_CPU_PERIOD = os.path.join(CGROUP_ROOT, "cpu", "cpu.cfs_period_us")
CGROUP_V1_MEMORY_LIMIT = os.path.join(CGROUP_ROOT, "memory", "memory.limit_in_bytes")
CGROUP_V1_MEMORY_USAGE = os.path.join(CGROUP_ROOT, "memory", "memory.usage_in_bytes")

PROC_CPUINFO = "/proc/cpuinfo"
PROC_MEMINFO = "/proc/meminfo"
PROC_MDSTAT = "/proc/mdstat"
PROC_MOUNTS = "/proc/mounts"

SYS_BLOCK = "/sys/block"
SYS_DMI_PRODUCT = "/sys/devices/virtual/dmi/id/product_name"

ENV_INSTANCE_TYPE = "TD_NODE_INSTANCE_TYPE"
ENV_NODE_NAME = "TD_NODE_NAME"

VIRTUAL_BLOCK_PREFIXES = ("loop", "ram", "nbd", "dm-", "sr", "zram", "fd")

SECTOR_BYTES = 512

CPU_MODEL_KEY = "model name"

MEMINFO_KEYS = ("MemTotal", "MemAvailable", "SwapTotal", "Hugepagesize")

REPORTED_PATHS = ("/", "/tmp", "/tabsdata")

THROTTLING_KEYS = ("nr_periods", "nr_throttled", "throttled_usec")

UNSET = "<unset>"
UNAVAILABLE = "<unavailable>"
UNLIMITED = "max"


def read_text(path: str) -> str | None:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            return handle.read().strip()
    except OSError:
        return None


def human_bytes(value: float) -> str:
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if size < 1024 or unit == "PiB":
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.2f} {unit}"
        size /= 1024
    raise AssertionError


def cgroup_version() -> str:
    return "v2" if os.path.exists(CGROUP_V2_MARKER) else "v1"


def instance_type() -> str:
    declared = os.environ.get(ENV_INSTANCE_TYPE, "").strip()
    if declared:
        return declared
    return read_text(SYS_DMI_PRODUCT) or UNAVAILABLE


def cpu_model() -> str:
    contents = read_text(PROC_CPUINFO)
    if not contents:
        return UNAVAILABLE
    for line in contents.splitlines():
        if line.startswith(CPU_MODEL_KEY):
            _, _, value = line.partition(":")
            return value.strip()
    return UNAVAILABLE


def cpu_quota() -> str:
    contents = read_text(CGROUP_V2_CPU_MAX)
    if contents:
        parts = contents.split()
        if parts and parts[0] == UNLIMITED:
            return "unlimited (no cpu ceiling)"
        if len(parts) == 2:
            quota, period = int(parts[0]), int(parts[1])
            return f"{quota / period:.3f} cores ({quota}us per {period}us)"
        return contents
    quota_text = read_text(CGROUP_V1_CPU_QUOTA)
    period_text = read_text(CGROUP_V1_CPU_PERIOD)
    if quota_text and period_text:
        quota, period = int(quota_text), int(period_text)
        if quota <= 0:
            return "unlimited (no cpu ceiling)"
        return f"{quota / period:.3f} cores ({quota}us per {period}us)"
    return UNAVAILABLE


def cpu_throttling() -> list[str]:
    contents = read_text(CGROUP_V2_CPU_STAT)
    if not contents:
        return []
    return [
        f"cgroup_cpu_{line}"
        for line in contents.splitlines()
        if line.startswith(THROTTLING_KEYS)
    ]


def memory_limit(path_v2: str, path_v1: str) -> str:
    contents = read_text(path_v2)
    if contents is None:
        contents = read_text(path_v1)
    if contents is None:
        return UNAVAILABLE
    if contents == UNLIMITED:
        return "unlimited (no memory ceiling)"
    value = int(contents)
    if value >= 1 << 62:
        return "unlimited (no memory ceiling)"
    return f"{human_bytes(value)} ({value} bytes)"


def node_memory() -> list[str]:
    contents = read_text(PROC_MEMINFO)
    if not contents:
        return []
    found: list[str] = []
    for line in contents.splitlines():
        name, _, value = line.partition(":")
        if name not in MEMINFO_KEYS:
            continue
        value = value.strip()
        kilobytes = value.split()[0] if value.endswith("kB") else None
        if kilobytes and kilobytes.isdigit():
            total = int(kilobytes) * 1024
            found.append(f"node_{name} = {human_bytes(total)} ({value})")
        else:
            found.append(f"node_{name} = {value}")
    return found


def block_devices() -> list[str]:
    devices: list[str] = []
    for name in sorted(os.listdir(SYS_BLOCK)):
        if name.startswith(VIRTUAL_BLOCK_PREFIXES):
            continue
        sectors = read_text(os.path.join(SYS_BLOCK, name, "size"))
        if not sectors or sectors == "0":
            continue
        rotational = read_text(os.path.join(SYS_BLOCK, name, "queue", "rotational"))
        kind = {"0": "ssd/nvme", "1": "rotational"}.get(rotational or "", "unknown")
        capacity = human_bytes(int(sectors) * SECTOR_BYTES)
        model = read_text(os.path.join(SYS_BLOCK, name, "device", "model"))
        scheduler = read_text(os.path.join(SYS_BLOCK, name, "queue", "scheduler"))
        detail = f"{kind}, scheduler={scheduler or UNAVAILABLE}"
        if model:
            detail = f"{detail}, model={model}"
        devices.append(f"device {name} = {capacity} ({detail})")
    return devices


def raid_status() -> list[str]:
    contents = read_text(PROC_MDSTAT)
    if contents is None:
        return ["raid = no /proc/mdstat (no software raid on this node)"]
    found = [line for line in contents.splitlines() if line.strip()]
    if not any(line.startswith("md") for line in found):
        return ["raid = /proc/mdstat present but no active md array"]
    return [f"raid: {line}" for line in found]


def disk_usage() -> list[str]:
    usage: list[str] = []
    for path in REPORTED_PATHS:
        if not os.path.isdir(path):
            continue
        try:
            total, used, free = shutil.disk_usage(path)
        except OSError as error:
            usage.append(f"usage {path} = <error: {error}>")
            continue
        usage.append(
            f"usage {path} = total={human_bytes(total)}"
            f" used={human_bytes(used)} free={human_bytes(free)}"
        )
    return usage


def mounts() -> list[str]:
    contents = read_text(PROC_MOUNTS)
    if not contents:
        return []
    found: list[str] = []
    for line in contents.splitlines():
        fields = line.split()
        if len(fields) < 3:
            continue
        device, mount_point, filesystem = fields[0], fields[1], fields[2]
        if mount_point in REPORTED_PATHS or mount_point.startswith("/tabsdata"):
            found.append(f"mount {mount_point} on {device} type {filesystem}")
    return found


def environment() -> list[str]:
    return [f"{name} = {os.environ[name]}" for name in sorted(os.environ)]


def report_system() -> None:
    lines: list[str] = []
    failures: list[str] = []

    def section(name: str) -> None:
        lines.append(f"--- system: {name} ---")

    def record(name: str, probe) -> None:
        try:
            value = probe()
        except Exception as error:
            failures.append(f"{name}\n{traceback.format_exc().strip()}")
            lines.append(f"{name} = <error: {type(error).__name__}: {error}>")
            return
        if isinstance(value, list):
            if not value:
                lines.append(f"{name} = {UNAVAILABLE}")
                return
            lines.extend(str(entry) for entry in value)
            return
        lines.append(f"{name} = {value}")

    try:
        section("identity")
        record("hostname", socket.gethostname)
        record("node_name", lambda: os.environ.get(ENV_NODE_NAME, UNSET))
        record("platform", platform.platform)
        record("machine", platform.machine)
        record("instance_type", instance_type)
        record("cpu_model", cpu_model)

        section("cpu")
        record("cgroup", cgroup_version)
        record("node_cpus", os.cpu_count)
        record("usable_cpus", lambda: len(os.sched_getaffinity(0)))
        record("cgroup_cpu_limit", cpu_quota)
        record("cgroup_cpu_throttling", cpu_throttling)

        section("memory")
        record("node_memory", node_memory)
        record(
            "cgroup_memory_limit",
            lambda: memory_limit(CGROUP_V2_MEMORY_MAX, CGROUP_V1_MEMORY_LIMIT),
        )
        record(
            "cgroup_memory_high",
            lambda: memory_limit(CGROUP_V2_MEMORY_HIGH, CGROUP_V1_MEMORY_LIMIT),
        )
        record(
            "cgroup_memory_current",
            lambda: memory_limit(CGROUP_V2_MEMORY_CURRENT, CGROUP_V1_MEMORY_USAGE),
        )

        section("disk")
        record("block_devices", block_devices)
        record("raid", raid_status)
        record("disk_usage", disk_usage)
        record("mounts", mounts)

        section("environment")
        record("environment", environment)

        if failures:
            section("gathering errors")
            for failure in failures:
                lines.extend(failure.splitlines())

        print("\n".join(lines), flush=True)
    except Exception:
        print(
            f"--- system: report failed ---\n{traceback.format_exc().strip()}",
            flush=True,
        )
