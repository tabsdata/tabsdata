#
#  Copyright 2026 Tabsdata Inc.
#

"""Dataclasses for the _meta.json v2 schema.

Pure data — no I/O, no pytest imports.  Every dataclass provides
``to_dict()`` for JSON serialization and a ``from_dict()`` class-method
for deserialization so that the monitor process can reconstruct state
from the persisted file.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

META_VERSION = 2


@dataclass
class PhaseState:
    status: str = "pending"  # "pending" | "active" | "done"
    start: float | None = None
    end: float | None = None
    duration: float | None = None

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "start": self.start,
            "end": self.end,
            "duration": self.duration,
        }

    @classmethod
    def from_dict(cls, data: dict) -> PhaseState:
        return cls(
            status=data.get("status", "pending"),
            start=data.get("start"),
            end=data.get("end"),
            duration=data.get("duration"),
        )


@dataclass
class FixtureState:
    scope: str = "function"
    lifecycle: str = "pending"
    autouse: bool = False
    total: int = 0
    remaining: int = 0
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    setup_count: int = 0
    teardown_count: int = 0
    in_setup: int = 0  # number of concurrent setups active right now
    in_teardown: int = 0  # number of concurrent teardowns active right now
    setup_ts: float | None = None
    teardown_ts: float | None = None

    def to_dict(self) -> dict:
        return {
            "scope": self.scope,
            "lifecycle": self.lifecycle,
            "autouse": self.autouse,
            "total": self.total,
            "remaining": self.remaining,
            "passed": self.passed,
            "failed": self.failed,
            "skipped": self.skipped,
            "setup_count": self.setup_count,
            "teardown_count": self.teardown_count,
            "in_setup": self.in_setup,
            "in_teardown": self.in_teardown,
            "setup_ts": self.setup_ts,
            "teardown_ts": self.teardown_ts,
        }

    @classmethod
    def from_dict(cls, data: dict) -> FixtureState:
        return cls(
            scope=data.get("scope", "function"),
            lifecycle=data.get("lifecycle", "pending"),
            autouse=data.get("autouse", False),
            total=data.get("total", 0),
            remaining=data.get("remaining", 0),
            passed=data.get("passed", 0),
            failed=data.get("failed", 0),
            skipped=data.get("skipped", 0),
            setup_count=data.get("setup_count", 0),
            teardown_count=data.get("teardown_count", 0),
            in_setup=data.get("in_setup", 0),
            in_teardown=data.get("in_teardown", 0),
            setup_ts=data.get("setup_ts"),
            teardown_ts=data.get("teardown_ts"),
        )


@dataclass
class MarkerState:
    total: int = 0
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    pending: int = 0

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "passed": self.passed,
            "failed": self.failed,
            "skipped": self.skipped,
            "pending": self.pending,
        }

    @classmethod
    def from_dict(cls, data: dict) -> MarkerState:
        return cls(
            total=data.get("total", 0),
            passed=data.get("passed", 0),
            failed=data.get("failed", 0),
            skipped=data.get("skipped", 0),
            pending=data.get("pending", 0),
        )


@dataclass
class ComponentState:
    total: int = 0
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    pending: int = 0

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "passed": self.passed,
            "failed": self.failed,
            "skipped": self.skipped,
            "pending": self.pending,
        }

    @classmethod
    def from_dict(cls, data: dict) -> ComponentState:
        return cls(
            total=data.get("total", 0),
            passed=data.get("passed", 0),
            failed=data.get("failed", 0),
            skipped=data.get("skipped", 0),
            pending=data.get("pending", 0),
        )


@dataclass
class PluginState:
    plugin_type: str = "third-party"  # "builtin" | "conftest" | "third-party"
    active: bool = False

    def to_dict(self) -> dict:
        return {
            "type": self.plugin_type,
            "active": self.active,
        }

    @classmethod
    def from_dict(cls, data: dict) -> PluginState:
        return cls(
            plugin_type=data.get("type", "third-party"),
            active=data.get("active", False),
        )


@dataclass
class MetaState:
    version: int = META_VERSION
    pid: int = 0
    collected: int = 0
    session_start_ts: float | None = None
    last_update_ts: float | None = None
    phases: dict[str, PhaseState] = field(default_factory=dict)
    fixtures: dict[str, FixtureState] = field(default_factory=dict)
    markers: dict[str, MarkerState] = field(default_factory=dict)
    components: dict[str, ComponentState] = field(default_factory=dict)
    plugins: dict[str, PluginState] = field(default_factory=dict)

    def to_dict(self) -> dict:
        self.last_update_ts = time.time()
        return {
            "version": self.version,
            "pid": self.pid,
            "collected": self.collected,
            "session_start_ts": self.session_start_ts,
            "last_update_ts": self.last_update_ts,
            "phases": {k: v.to_dict() for k, v in self.phases.items()},
            "fixtures": {k: v.to_dict() for k, v in self.fixtures.items()},
            "markers": {k: v.to_dict() for k, v in self.markers.items()},
            "components": {k: v.to_dict() for k, v in self.components.items()},
            "plugins": {k: v.to_dict() for k, v in self.plugins.items()},
        }

    @classmethod
    def from_dict(cls, data: dict) -> MetaState:
        return cls(
            version=data.get("version", META_VERSION),
            pid=data.get("pid", 0),
            collected=data.get("collected", 0),
            session_start_ts=data.get("session_start_ts"),
            last_update_ts=data.get("last_update_ts"),
            phases={
                k: PhaseState.from_dict(v) for k, v in data.get("phases", {}).items()
            },
            fixtures={
                k: FixtureState.from_dict(v)
                for k, v in data.get("fixtures", {}).items()
            },
            markers={
                k: MarkerState.from_dict(v) for k, v in data.get("markers", {}).items()
            },
            components={
                k: ComponentState.from_dict(v)
                for k, v in data.get("components", {}).items()
            },
            plugins={
                k: PluginState.from_dict(v) for k, v in data.get("plugins", {}).items()
            },
        )
