#
# Copyright 2026 Tabsdata Inc.
#

"""A monkey run across an upgrade: a released tabsdata runs the first part of a
seeded plan, the instance stops with work in flight and is upgraded to the
running build, and the second part adopts the project and settles it. The run
then holds as any monkey run does, over executions on both sides of the
upgrade, and the project's topology reads at every version the upgrade
numbered. How long accepted operations took before and after is reported, not
judged.

The test owns its instance: it creates it with the release's ``tdkserver``,
upgrades it with this one, and trashes it at the end, so no other instance may
hold the default ports. Opt in by naming the release's virtual environment,
which has ``tabsdata`` and ``tabsdata-agent`` of that release installed, and
``kubernetes`` for the faults (releases before 2.1 lack it):
``TD_UPGRADE_FROM=~/.pyenv/versions/td-2.1.0``.

Settings: ``TD_UPGRADE_SEED``, ``TD_UPGRADE_OPS`` (120), ``TD_UPGRADE_LANES``
(4), ``TD_UPGRADE_SCALE`` (0.25), ``TD_UPGRADE_MAX_SECONDS`` (20),
``TD_UPGRADE_FAULTS`` (1), ``TD_UPGRADE_SPLIT`` (0.5, the share of operations
before the upgrade), ``TD_UPGRADE_PLAN`` (a plan file to replay instead of a
seed), ``TD_UPGRADE_KEEP`` (1 keeps the instance), ``TD_UPGRADE_TIMEOUT``
(3600 seconds) and ``TD_UPGRADE_PROGRESS`` (as ``MONKEY_PROGRESS``)."""

from __future__ import annotations

import os
import pty
import random
import select
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest

from tabsdatak.extensions.variant.edition import ENTERPRISE
from testi_tabsdatak.conn.common.cluster import Cluster
from testi_tabsdatak.conn.common.monkey import upgrade
from testi_tabsdatak.conn.common.monkey.descriptor import load_plan, write_plan
from testi_tabsdatak.conn.common.monkey.naming import base36, run_namespace
from testi_tabsdatak.conn.common.monkey.plan import generate
from testi_tabsdatak.conn.common.monkey.progress import FAILED, SETUP, Progress
from testi_tabsdatak.conn.common.monkey.project import write_project
from testi_tabsdatak.conn.common.monkey.runner import MonkeyRun

RELEASE = os.environ.get("TD_UPGRADE_FROM")
SEED = os.environ.get("TD_UPGRADE_SEED")
OPERATIONS = int(os.environ.get("TD_UPGRADE_OPS", "120"))
LANES = int(os.environ.get("TD_UPGRADE_LANES", "4"))
SCALE = float(os.environ.get("TD_UPGRADE_SCALE", "0.25"))
MAX_SECONDS = int(os.environ.get("TD_UPGRADE_MAX_SECONDS", "20"))
FAULTS = os.environ.get("TD_UPGRADE_FAULTS", "1") != "0"
SPLIT = float(os.environ.get("TD_UPGRADE_SPLIT", "0.5"))
PLAN = os.environ.get("TD_UPGRADE_PLAN")
KEEP = os.environ.get("TD_UPGRADE_KEEP", "0") != "0"
TIMEOUT = int(os.environ.get("TD_UPGRADE_TIMEOUT", "3600"))
PROGRESS = os.environ.get("TD_UPGRADE_PROGRESS")

FLEET_MODULE = Path(__file__).parent / "monkey" / "monkey_fleet.py"
ENGINE_ROOT = Path(upgrade.__file__).parents[4]
INSTANCES = Path.home() / ".tabsdata" / "instances"
SERVER_TIMEOUT = 1800
QUESTION = b"[y/yes/n/no]:"
PHASE_SHARE = 0.4
REPORTED_PROBLEMS = 30
TIMING_COLUMNS = ("Action", "Before", "p50", "p90", "p99", "After")

pytestmark = [
    pytest.mark.sdk,
    pytest.mark.slow,
    pytest.mark.monkey,
    pytest.mark.xdist_group("monkey_seq"),
    pytest.mark.skipif(
        not RELEASE, reason="upgrade runs need TD_UPGRADE_FROM, a release venv"
    ),
    pytest.mark.skipif(
        not ENTERPRISE,
        reason="monkey runs need downstream functions triggered as their inputs change",
    ),
]


@pytest.fixture
def progress() -> Iterator[Progress]:
    reporter = Progress.open(PROGRESS)
    yield reporter
    reporter.close()


def environment(bin_dir: Path) -> dict[str, str]:
    """``tdkserver`` finds its helper programs on the path, so the build that
    runs must come first there."""
    path = f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}"
    return dict(os.environ, PATH=path, TDK_IN_AGENT="1")


def in_terminal(argv: list[str], env: dict[str, str]) -> tuple[int, str]:
    """Runs ``argv`` on a pseudo-terminal, answering yes to every question:
    some releases refuse to run outside a terminal, and ask some questions,
    ``--yes`` or not."""
    controller, terminal = pty.openpty()
    process = subprocess.Popen(
        argv, env=env, stdin=terminal, stdout=terminal, stderr=terminal
    )
    os.close(terminal)
    output = bytearray()
    deadline = time.monotonic() + SERVER_TIMEOUT
    try:
        while time.monotonic() < deadline:
            if select.select([controller], [], [], 1)[0]:
                try:
                    chunk = os.read(controller, 65536)
                except OSError:
                    break
                if not chunk:
                    break
                output += chunk
                if output.rstrip().endswith(QUESTION):
                    os.write(controller, b"y\n")
            elif process.poll() is not None:
                break
        else:
            process.kill()
        return process.wait(), output.decode(errors="replace")
    finally:
        os.close(controller)


def tdkserver(bin_dir: Path, *args: str, release: bool) -> None:
    """A release's ``tdkserver`` resolves its environments from PyPI, so it
    runs without ``TD_KUBE_UV_MODE``; this build's keeps it."""
    env = environment(bin_dir)
    if release:
        env.pop("TD_KUBE_UV_MODE", None)
    code, output = in_terminal(
        [str(bin_dir / "tdkserver"), "--accept-terms=y", *args], env
    )
    if code:
        raise AssertionError(
            f"tdkserver {' '.join(args)} failed ({code}): {output[-2000:]}"
        )


def run_before(directory: Path, folder: Path, release: Path) -> dict:
    env = dict(environment(release / "bin"), PYTHONPATH=str(ENGINE_ROOT))
    result = subprocess.run(
        [str(release / "bin" / "python"), "-m", upgrade.__name__, str(directory)]
        + [str(folder)],
        env=env,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=TIMEOUT,
    )
    if result.returncode:
        output = (result.stdout + result.stderr)[-3000:]
        raise AssertionError(f"The run before the upgrade failed: {output}")
    return upgrade.read_yaml(directory / upgrade.BEFORE_FILE)


def seconds(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f} s"


def timing_rows(before: list[dict], after: list) -> list[tuple[str, ...]]:
    earlier = upgrade.timings(before)
    later = upgrade.timings([outcome.to_dict() for outcome in after])
    rows = []
    for action in sorted(set(earlier) | set(later)):
        old, new = earlier.get(action, {}), later.get(action, {})
        rows.append((
            action,
            str(old.get("count", 0)),
            *(
                f"{seconds(old.get(rank))} -> {seconds(new.get(rank))}"
                for rank in ("p50", "p90", "p99")
            ),
            str(new.get("count", 0)),
        ))
    return rows


@pytest.mark.timeout(TIMEOUT)
def test_a_monkey_run_holds_across_an_upgrade(tmp_path: Path, progress: Progress):
    release = Path(RELEASE).expanduser()
    if PLAN:
        plan = load_plan(Path(PLAN))
    else:
        seed = int(SEED) if SEED else random.SystemRandom().randrange(2**32)
        plan = upgrade.without_streams(
            generate(seed, OPERATIONS, LANES, FAULTS, SCALE, MAX_SECONDS)
        )
    first, second = upgrade.split(plan, SPLIT)
    namespace = run_namespace()
    project = f"monkey_{namespace}"
    name = f"upgrade{base36(int(time.time()))}"
    folder = INSTANCES / name
    progress.layout(len(plan.ops), (spec.name for spec in plan.fleet.functions))
    full = write_plan(tmp_path / "full.yaml", plan)
    progress(f"Plan (seed {plan.seed}) written to {full}", SETUP)
    write_plan(tmp_path / upgrade.PLAN_FILE, first)
    upgrade.write_yaml(
        tmp_path / upgrade.SETUP_FILE,
        {
            "project": project,
            "namespace": namespace,
            "fleet_module": str(FLEET_MODULE),
            "timeout": int(TIMEOUT * PHASE_SHARE),
            "progress": PROGRESS,
        },
    )
    problems: list[str] = []
    owner = (release / "bin", True)
    try:
        progress(f"Creating instance {name} on {release.name}", SETUP)
        tdkserver(release / "bin", "quickstart", "--instance", name, "-y", release=True)
        progress(f"Running {len(first.ops)} operations on {release.name}", SETUP)
        before = run_before(tmp_path, folder, release)
        problems += [f"Before the upgrade: {problem}" for problem in before["problems"]]
        progress(f"Stopping {name} with work in flight", SETUP)
        tdkserver(release / "bin", "stop", "--instance", name, "-y", release=True)
        current = Path(sys.executable).parent
        progress(f"Upgrading {name} to this build", SETUP)
        tdkserver(current, "upgrade", "--instance", name, "--yes", release=False)
        owner = (current, False)
        tdkserver(current, "start", "--instance", name, "--yes", release=False)
        settings = upgrade.instance_settings(folder)
        port = upgrade.apiserver_port(settings)
        root = tmp_path / "project"
        report = MonkeyRun(
            plan=second,
            namespace=namespace,
            connect=lambda: upgrade.connect(port, project),
            cluster=Cluster(SimpleNamespace(settings=settings)),
            project=project,
            project_id=before["project_id"],
            project_root=root,
            module=write_project(root, plan.fleet, namespace, FLEET_MODULE),
            progress=progress,
            deadline=time.monotonic() + TIMEOUT * PHASE_SHARE,
            adopt=True,
        ).execute()
        problems += report.problems
        problems += upgrade.graph_problems(upgrade.connect(port, project), project)
        progress.table(TIMING_COLUMNS, timing_rows(before["outcomes"], report.outcomes))
    except Exception as error:
        problems.append(f"Run aborted: {type(error).__name__}: {error}")
    finally:
        if not KEEP and folder.exists():
            bin_dir, is_release = owner
            for command in ("stop", "trash"):
                try:
                    tdkserver(
                        bin_dir, command, "--instance", name, "-y", release=is_release
                    )
                except Exception as error:
                    progress(f"Could not {command} {name}: {error}", FAILED)
    if problems:
        shown = problems[:REPORTED_PROBLEMS]
        if len(problems) > REPORTED_PROBLEMS:
            shown.append(f"And {len(problems) - REPORTED_PROBLEMS} more problems")
        pytest.fail(
            "\n".join([
                f"Upgrade run from {release.name} failed; plan: {full}",
                *shown,
            ]),
            pytrace=False,
        )
