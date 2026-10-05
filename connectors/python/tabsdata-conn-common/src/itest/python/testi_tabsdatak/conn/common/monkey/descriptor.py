#
# Copyright 2026 Tabsdata Inc.
#

"""Monkey run descriptors: the plan of a failed run, kept next to the test so
it can be committed and replayed on every later run, together with what the
run expected and what it found."""

from __future__ import annotations

import time
from pathlib import Path

import yaml

from testi_tabsdatak.conn.common.monkey.plan import Plan
from testi_tabsdatak.conn.common.monkey.runner import Report

FORMAT = 1
SUFFIX = ".yaml"


def header() -> str:
    return f"#\n# Copyright {time.localtime().tm_year} Tabsdata Inc.\n#\n\n"


def dump(path: Path, data: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(header() + yaml.safe_dump(data, sort_keys=False, width=100))
    return path


def stored(directory: Path) -> list[Path]:
    return sorted(directory.glob(f"*{SUFFIX}")) if directory.is_dir() else []


def load_plan(path: Path) -> Plan:
    return Plan.from_dict(yaml.safe_load(path.read_text())["plan"])


def write_plan(path: Path, plan: Plan) -> Path:
    return dump(path, {"format": FORMAT, "plan": plan.to_dict()})


def write_failure(directory: Path, plan: Plan, namespace: str, report: Report) -> Path:
    name = f"{time.strftime('%Y%m%d-%H%M%S')}-{plan.seed}{SUFFIX}"
    return dump(
        directory / name,
        {
            "format": FORMAT,
            "namespace": namespace,
            "failure": {
                "duration": report.duration,
                "problems": report.problems,
                "observed": report.observed,
            },
            "expected": report.expected,
            "plan": plan.to_dict(),
            "outcomes": [outcome.to_dict() for outcome in report.outcomes],
        },
    )
