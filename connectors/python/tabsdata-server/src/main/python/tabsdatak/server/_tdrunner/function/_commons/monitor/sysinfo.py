#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import platform
import shutil
import socket
import traceback
from typing import Any, Callable

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


class SystemReport:
    lines: list[str]
    failures: list[str]

    def __init__(self) -> None:
        self.lines = []
        self.failures = []

    def show(self) -> None:
        try:
            self.gather()
            print("\n".join(self.lines), flush=True)
        except Exception:
            print(
                f"--- system: report failed ---\n{traceback.format_exc().strip()}",
                flush=True,
            )

    def gather(self) -> None:
        self.section("identity")
        self.record("hostname", socket.gethostname)
        self.record("node_name", lambda: os.environ.get(ENV_NODE_NAME, UNSET))
        self.record("platform", platform.platform)
        self.record("machine", platform.machine)
        self.record("instance_type", self.instance_type)
        self.record("cpu_model", self.cpu_model)

        self.section("cpu")
        self.record("cgroup", self.cgroup_version)
        self.record("node_cpus", os.cpu_count)
        self.record("usable_cpus", self.usable_cpus)
        self.record("cgroup_cpu_limit", self.cpu_quota)
        self.record("cgroup_cpu_throttling", self.cpu_throttling)

        self.section("memory")
        self.record("node_memory", self.node_memory)
        self.record(
            "cgroup_memory_limit",
            lambda: self.memory_limit(CGROUP_V2_MEMORY_MAX, CGROUP_V1_MEMORY_LIMIT),
        )
        self.record(
            "cgroup_memory_high",
            lambda: self.memory_limit(CGROUP_V2_MEMORY_HIGH, CGROUP_V1_MEMORY_LIMIT),
        )
        self.record(
            "cgroup_memory_current",
            lambda: self.memory_limit(
                CGROUP_V2_MEMORY_CURRENT,
                CGROUP_V1_MEMORY_USAGE,
            ),
        )

        self.section("disk")
        self.record("block_devices", self.block_devices)
        self.record("raid", self.raid_status)
        self.record("disk_usage", self.disk_usage)
        self.record("mounts", self.mounts)

        if self.failures:
            self.section("gathering errors")
            for failure in self.failures:
                self.lines.extend(failure.splitlines())

    def section(self, name: str) -> None:
        self.lines.append(f"--- system: {name} ---")

    def record(self, name: str, probe: Callable[[], Any]) -> None:
        try:
            value = probe()
        except Exception as error:
            self.failures.append(f"{name}\n{traceback.format_exc().strip()}")
            self.lines.append(f"{name} = <error: {type(error).__name__}: {error}>")
            return
        if isinstance(value, list):
            if not value:
                self.lines.append(f"{name} = {UNAVAILABLE}")
                return
            self.lines.extend(str(entry) for entry in value)
            return
        self.lines.append(f"{name} = {value}")

    @staticmethod
    def read_text(path: str) -> str | None:
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as handle:
                return handle.read().strip()
        except OSError:
            return None

    @staticmethod
    def human_bytes(value: float) -> str:
        size = float(value)
        for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
            if size < 1024 or unit == "PiB":
                if unit == "B":
                    return f"{int(size)} {unit}"
                return f"{size:.2f} {unit}"
            size /= 1024
        raise AssertionError

    @staticmethod
    def cgroup_version() -> str:
        return "v2" if os.path.exists(CGROUP_V2_MARKER) else "v1"

    @staticmethod
    def instance_type() -> str:
        declared = os.environ.get(ENV_INSTANCE_TYPE, "").strip()
        if declared:
            return declared
        return SystemReport.read_text(SYS_DMI_PRODUCT) or UNAVAILABLE

    @staticmethod
    def cpu_model() -> str:
        contents = SystemReport.read_text(PROC_CPUINFO)
        if not contents:
            return UNAVAILABLE
        for line in contents.splitlines():
            if line.startswith(CPU_MODEL_KEY):
                _, _, value = line.partition(":")
                return value.strip()
        return UNAVAILABLE

    @staticmethod
    def cpu_quota() -> str:
        contents = SystemReport.read_text(CGROUP_V2_CPU_MAX)
        if contents:
            parts = contents.split()
            if parts and parts[0] == UNLIMITED:
                return "unlimited (no cpu ceiling)"
            if len(parts) == 2:
                quota, period = int(parts[0]), int(parts[1])
                return f"{quota / period:.3f} cores ({quota}us per {period}us)"
            return contents
        quota_text = SystemReport.read_text(CGROUP_V1_CPU_QUOTA)
        period_text = SystemReport.read_text(CGROUP_V1_CPU_PERIOD)
        if quota_text and period_text:
            quota, period = int(quota_text), int(period_text)
            if quota <= 0:
                return "unlimited (no cpu ceiling)"
            return f"{quota / period:.3f} cores ({quota}us per {period}us)"
        return UNAVAILABLE

    @staticmethod
    def cpu_throttling() -> list[str]:
        contents = SystemReport.read_text(CGROUP_V2_CPU_STAT)
        if not contents:
            return []
        return [
            f"cgroup_cpu_{line}"
            for line in contents.splitlines()
            if line.startswith(THROTTLING_KEYS)
        ]

    @staticmethod
    def memory_limit(path_v2: str, path_v1: str) -> str:
        contents = SystemReport.read_text(path_v2)
        if contents is None:
            contents = SystemReport.read_text(path_v1)
        if contents is None:
            return UNAVAILABLE
        if contents == UNLIMITED:
            return "unlimited (no memory ceiling)"
        value = int(contents)
        if value >= 1 << 62:
            return "unlimited (no memory ceiling)"
        return f"{SystemReport.human_bytes(value)} ({value} bytes)"

    @staticmethod
    def node_memory() -> list[str]:
        contents = SystemReport.read_text(PROC_MEMINFO)
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
                found.append(
                    f"node_{name} = {SystemReport.human_bytes(total)} ({value})"
                )
            else:
                found.append(f"node_{name} = {value}")
        return found

    @staticmethod
    def usable_cpus() -> int | str:
        try:
            return len(os.sched_getaffinity(0))
        except (AttributeError, OSError):
            return UNAVAILABLE

    @staticmethod
    def block_devices() -> list[str]:
        devices: list[str] = []
        if not os.path.isdir(SYS_BLOCK):
            return devices
        for name in sorted(os.listdir(SYS_BLOCK)):
            if name.startswith(VIRTUAL_BLOCK_PREFIXES):
                continue
            sectors = SystemReport.read_text(os.path.join(SYS_BLOCK, name, "size"))
            if not sectors or sectors == "0":
                continue
            rotational = SystemReport.read_text(
                os.path.join(SYS_BLOCK, name, "queue", "rotational")
            )
            kind = {"0": "ssd/nvme", "1": "rotational"}.get(rotational or "", "unknown")
            capacity = SystemReport.human_bytes(int(sectors) * SECTOR_BYTES)
            model = SystemReport.read_text(
                os.path.join(SYS_BLOCK, name, "device", "model")
            )
            scheduler = SystemReport.read_text(
                os.path.join(SYS_BLOCK, name, "queue", "scheduler")
            )
            detail = f"{kind}, scheduler={scheduler or UNAVAILABLE}"
            if model:
                detail = f"{detail}, model={model}"
            devices.append(f"device {name} = {capacity} ({detail})")
        return devices

    @staticmethod
    def raid_status() -> list[str]:
        contents = SystemReport.read_text(PROC_MDSTAT)
        if contents is None:
            return ["raid = no /proc/mdstat (no software raid on this node)"]
        found = [line for line in contents.splitlines() if line.strip()]
        if not any(line.startswith("md") for line in found):
            return ["raid = /proc/mdstat present but no active md array"]
        return [f"raid: {line}" for line in found]

    @staticmethod
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
                f"usage {path} = total={SystemReport.human_bytes(total)}"
                f" used={SystemReport.human_bytes(used)}"
                f" free={SystemReport.human_bytes(free)}"
            )
        return usage

    @staticmethod
    def mounts() -> list[str]:
        contents = SystemReport.read_text(PROC_MOUNTS)
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
