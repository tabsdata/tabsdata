#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Callable, Mapping

import psutil

TD_DATAFLOW_PREFIX = "TD_DATAFLOW__"
TD_DATAFLOW__TEMPORARY_FOLDER = "TD_DATAFLOW__TEMPORARY_FOLDER"
TD_DATAFLOW__SPILL__ENABLED = "TD_DATAFLOW__SPILL__ENABLED"
TD_DATAFLOW__SPILL__FOLDER = "TD_DATAFLOW__SPILL__FOLDER"
TD_DATAFLOW__SPILL__MEMORY_THRESHOLD_PERCENT = (
    "TD_DATAFLOW__SPILL__MEMORY_THRESHOLD_PERCENT"
)
TD_DATAFLOW__SPILL__DISK_LIMIT_PERCENT = "TD_DATAFLOW__SPILL__DISK_LIMIT_PERCENT"
TD_DATAFLOW__ABORT__MEMORY_THRESHOLD_PERCENT = (
    "TD_DATAFLOW__ABORT__MEMORY_THRESHOLD_PERCENT"
)
CONTAINER_ROOT = "CONTAINER_ROOT"

POLARS_TEMP_DIR = "POLARS_TEMP_DIR"
POLARS_OVERRIDE_TOTAL_MEMORY_MB = "POLARS_OVERRIDE_TOTAL_MEMORY_MB"
POLARS_OOC_SPILL_DIR = "POLARS_OOC_SPILL_DIR"
POLARS_OOC_MEMORY_BUDGET_FRACTION = "POLARS_OOC_MEMORY_BUDGET_FRACTION"
POLARS_OOC_DISK_BUDGET_MB = "POLARS_OOC_DISK_BUDGET_MB"
POLARS_OOMKILL_THRESHOLD_MB = "POLARS_OOMKILL_THRESHOLD_MB"
POLARS_VARIABLES = (
    POLARS_TEMP_DIR,
    POLARS_OVERRIDE_TOTAL_MEMORY_MB,
    POLARS_OOC_SPILL_DIR,
    POLARS_OOC_MEMORY_BUDGET_FRACTION,
    POLARS_OOC_DISK_BUDGET_MB,
    POLARS_OOMKILL_THRESHOLD_MB,
)

DEFAULT_TEMPORARY_FOLDER = "/tabsdata/dataflow/temporary"
DEFAULT_SPILL_FOLDER = "/tabsdata/dataflow/spill"
DEFAULT_SPILL_MEMORY_THRESHOLD_PERCENT = 40
DEFAULT_SPILL_DISK_LIMIT_PERCENT = 80
DEFAULT_ABORT_MEMORY_THRESHOLD_PERCENT = 95

NO_SPILL_FRACTION = "1.0"
UNWRITABLE_SPILL_FOLDER = os.path.join(os.devnull, "spill")

PROCESS_CGROUP = Path("/proc/self/cgroup")
CGROUP_ROOT = Path("/sys/fs/cgroup")
CGROUP_V1_MEMORY_CONTROLLER = "memory"
CGROUP_V2_MEMORY_LIMIT = "memory.max"
CGROUP_V1_MEMORY_LIMIT = "memory.limit_in_bytes"
CGROUP_V2_HIERARCHY = "0"
CGROUP_UNLIMITED = "max"
CGROUP_SEPARATOR = ":"
CGROUP_CONTROLLER_SEPARATOR = ","

MEGABYTE = 1_000_000
PERCENT = 100
TRUE = "true"
FALSE = "false"


class Dataflow:
    def __init__(self, environment: Mapping[str, str] | None = None) -> None:
        environment = os.environ if environment is None else environment
        root = environment.get(CONTAINER_ROOT, "")
        self.configured = any(
            name.startswith(TD_DATAFLOW_PREFIX) for name in environment
        )
        self.temporary_folder = Path(
            root
            + environment.get(TD_DATAFLOW__TEMPORARY_FOLDER, DEFAULT_TEMPORARY_FOLDER)
        )
        self.spill_enabled = self._flag(environment, TD_DATAFLOW__SPILL__ENABLED, True)
        self.spill_folder = Path(
            root + environment.get(TD_DATAFLOW__SPILL__FOLDER, DEFAULT_SPILL_FOLDER)
        )
        self.spill_memory_threshold_percent = self._percent(
            environment,
            TD_DATAFLOW__SPILL__MEMORY_THRESHOLD_PERCENT,
            DEFAULT_SPILL_MEMORY_THRESHOLD_PERCENT,
        )
        self.spill_disk_limit_percent = self._percent(
            environment,
            TD_DATAFLOW__SPILL__DISK_LIMIT_PERCENT,
            DEFAULT_SPILL_DISK_LIMIT_PERCENT,
        )
        self.abort_memory_threshold_percent = self._percent(
            environment,
            TD_DATAFLOW__ABORT__MEMORY_THRESHOLD_PERCENT,
            DEFAULT_ABORT_MEMORY_THRESHOLD_PERCENT,
        )

    @staticmethod
    def settings_in_effect(
        environment: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        environment = os.environ if environment is None else environment
        return {
            name: environment[name] for name in POLARS_VARIABLES if name in environment
        }

    def folders(self) -> list[Path]:
        if self.spill_enabled:
            return [self.temporary_folder, self.spill_folder]
        return [self.temporary_folder]

    def polars_environment(
        self,
        memory_bytes: int,
        free_bytes: Callable[[Path], int],
    ) -> dict[str, str]:
        environment = {
            POLARS_TEMP_DIR: str(self.temporary_folder),
            POLARS_OVERRIDE_TOTAL_MEMORY_MB: self._megabytes(memory_bytes, PERCENT),
        }
        if self.spill_enabled:
            environment[POLARS_OOC_SPILL_DIR] = str(self.spill_folder)
            environment[POLARS_OOC_MEMORY_BUDGET_FRACTION] = str(
                self.spill_memory_threshold_percent / PERCENT
            )
            environment[POLARS_OOC_DISK_BUDGET_MB] = self._megabytes(
                free_bytes(self.spill_folder), self.spill_disk_limit_percent
            )
        else:
            environment[POLARS_OOC_SPILL_DIR] = UNWRITABLE_SPILL_FOLDER
            environment[POLARS_OOC_MEMORY_BUDGET_FRACTION] = NO_SPILL_FRACTION
        environment[POLARS_OOMKILL_THRESHOLD_MB] = self._megabytes(
            memory_bytes, self.abort_memory_threshold_percent
        )
        return environment

    def apply(self) -> None:
        if not self.configured:
            return
        for folder in self.folders():
            folder.mkdir(parents=True, exist_ok=True)
        computed = self.polars_environment(MemoryLimit().in_bytes(), self._free_bytes)
        for name, value in computed.items():
            os.environ.setdefault(name, value)

    @staticmethod
    def _flag(environment: Mapping[str, str], name: str, default: bool) -> bool:
        value = environment.get(name)
        if value is None:
            return default
        normalized = value.strip().lower()
        if normalized == TRUE:
            return True
        if normalized == FALSE:
            return False
        raise ValueError(
            f"The setting '{name}' must be '{TRUE}' or '{FALSE}', not '{value}'"
        )

    @staticmethod
    def _percent(environment: Mapping[str, str], name: str, default: int) -> int:
        value = environment.get(name)
        if value is None:
            return default
        try:
            number = int(value)
        except ValueError:
            raise ValueError(
                f"The setting '{name}' must be a whole number, not '{value}'"
            ) from None
        if not 1 <= number <= PERCENT:
            raise ValueError(
                f"The setting '{name}' must be from 1 to {PERCENT}, not {number}"
            )
        return number

    @staticmethod
    def _megabytes(total_bytes: int, share_percent: int) -> str:
        return str(total_bytes * share_percent // PERCENT // MEGABYTE)

    @staticmethod
    def _free_bytes(folder: Path) -> int:
        return shutil.disk_usage(folder).free


class MemoryLimit:
    def __init__(
        self,
        process_cgroup: Path = PROCESS_CGROUP,
        cgroup_root: Path = CGROUP_ROOT,
    ) -> None:
        self.process_cgroup = process_cgroup
        self.cgroup_root = cgroup_root

    def in_bytes(self) -> int:
        physical = self.physical()
        limit = self.cgroup()
        if limit is None:
            return physical
        return min(limit, physical)

    def physical(self) -> int:
        return psutil.virtual_memory().total

    def cgroup(self) -> int | None:
        membership = self._membership()
        if membership is None:
            return None
        v1_root = self.cgroup_root / CGROUP_V1_MEMORY_CONTROLLER
        limits = [
            limit
            for root, limit_file in (
                (self.cgroup_root, CGROUP_V2_MEMORY_LIMIT),
                (v1_root, CGROUP_V1_MEMORY_LIMIT),
            )
            for folder in self._lineage(root / membership, root)
            if (limit := self._read_limit(folder / limit_file)) is not None
        ]
        return min(limits, default=None)

    def _membership(self) -> Path | None:
        content = self._read_text(self.process_cgroup)
        if content is None:
            return None
        for line in content.splitlines():
            hierarchy, _, rest = line.partition(CGROUP_SEPARATOR)
            controllers, _, path = rest.partition(CGROUP_SEPARATOR)
            unified = hierarchy == CGROUP_V2_HIERARCHY and not controllers
            if unified or CGROUP_V1_MEMORY_CONTROLLER in controllers.split(
                CGROUP_CONTROLLER_SEPARATOR
            ):
                return Path(path.lstrip("/"))
        return None

    @staticmethod
    def _lineage(folder: Path, root: Path) -> list[Path]:
        if not folder.is_relative_to(root):
            return [root]
        folders = [folder, *folder.parents]
        return folders[: folders.index(root) + 1]

    @classmethod
    def _read_limit(cls, path: Path) -> int | None:
        text = cls._read_text(path)
        if text is None or text == CGROUP_UNLIMITED:
            return None
        try:
            return int(text)
        except ValueError:
            return None

    @staticmethod
    def _read_text(path: Path) -> str | None:
        try:
            return path.read_text().strip()
        except OSError:
            return None
