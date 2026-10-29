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

Each run owns its instance: the release's ``tdkserver`` starts it on a claimed
port window, as the launch rulebook of the edition says for that release
(``ITEST_MONKEY_UPGRADE_LAUNCH_CONFIG``), this build's ``tdkserver`` upgrades
it, and the run trashes it at the end. The release comes from one of:

- ``TD_MONKEY_UPGRADE_FROM_VERSIONS``: comma-separated releases, one run each,
  each installed into a throwaway environment under
  ``ITEST_MONKEY_UPGRADE_VENVS_FOLDER``, deleted at the end.
- ``TD_MONKEY_UPGRADE_FROM_VENV``: an environment that already holds the
  release, used as it is and never deleted.

Setting both is an error, and with neither the test is skipped. Upgrade runs
take place in tabsdata-ee only.

Settings: ``TD_MONKEY_UPGRADE_SEED``, ``TD_MONKEY_UPGRADE_OPERATIONS`` (120),
``TD_MONKEY_UPGRADE_LANES`` (4), ``TD_MONKEY_UPGRADE_FLEET_SCALE`` (0.25),
``TD_MONKEY_UPGRADE_LONGEST_SLEEP_SECONDS`` (20), ``TD_MONKEY_UPGRADE_FAULTS``
(1), ``TD_MONKEY_UPGRADE_SPLIT`` (0.5, the share of operations before the
upgrade), ``TD_MONKEY_UPGRADE_PLAN`` (a plan file to replay instead of a seed),
``TD_MONKEY_UPGRADE_KEEP`` (1 keeps the instance and the environment),
``TD_MONKEY_UPGRADE_TIMEOUT_SECONDS`` (3600) and ``TD_MONKEY_UPGRADE_PROGRESS``
(as ``TD_MONKEY_PROGRESS``)."""

from __future__ import annotations

import codecs
import importlib.metadata
import os
import random
import re
import select
import shutil
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from itertools import takewhile
from pathlib import Path
from types import SimpleNamespace

import pytest

from tabsdatak.common._interpreter.python.venv import PythonEnvironment
from tabsdatak.extensions.variant.edition import ENTERPRISE
from testi_tabsdatak.conn.common.cluster import Cluster
from testi_tabsdatak.conn.common.monkey import release, upgrade
from testi_tabsdatak.conn.common.monkey.descriptor import load_plan, write_plan
from testi_tabsdatak.conn.common.monkey.naming import run_namespace
from testi_tabsdatak.conn.common.monkey.plan import generate
from testi_tabsdatak.conn.common.monkey.progress import FAILED, SETUP, Progress
from testi_tabsdatak.conn.common.monkey.project import write_project
from testi_tabsdatak.conn.common.monkey.runner import MonkeyRun

FROM_VERSIONS = os.environ.get("TD_MONKEY_UPGRADE_FROM_VERSIONS", "")
FROM_VENV = os.environ.get("TD_MONKEY_UPGRADE_FROM_VENV", "")
SEED = os.environ.get("TD_MONKEY_UPGRADE_SEED")
OPERATIONS = int(os.environ.get("TD_MONKEY_UPGRADE_OPERATIONS", "120"))
LANES = int(os.environ.get("TD_MONKEY_UPGRADE_LANES", "4"))
FLEET_SCALE = float(os.environ.get("TD_MONKEY_UPGRADE_FLEET_SCALE", "0.25"))
LONGEST_SLEEP_SECONDS = int(
    os.environ.get("TD_MONKEY_UPGRADE_LONGEST_SLEEP_SECONDS", "20"),
)
FAULTS = os.environ.get("TD_MONKEY_UPGRADE_FAULTS", "1") != "0"
SPLIT = float(os.environ.get("TD_MONKEY_UPGRADE_SPLIT", "0.5"))
PLAN = os.environ.get("TD_MONKEY_UPGRADE_PLAN")
KEEP = os.environ.get("TD_MONKEY_UPGRADE_KEEP", "0") != "0"
TIMEOUT = int(os.environ.get("TD_MONKEY_UPGRADE_TIMEOUT_SECONDS", "3600"))
PROGRESS = os.environ.get("TD_MONKEY_UPGRADE_PROGRESS")

LAUNCH_CONFIG = "ITEST_MONKEY_UPGRADE_LAUNCH_CONFIG"
REQUIREMENTS = "ITEST_MONKEY_UPGRADE_REQUIREMENTS"
VENVS_FOLDER = "ITEST_MONKEY_UPGRADE_VENVS_FOLDER"
PORTS_LOCK = "ITEST_PORTS_LOCK"
REGISTRY_FILE = "ITEST_REGISTRY_FILE"
UV_MODE = "TD_KUBE_UV_MODE"

FLEET_MODULE = Path(__file__).parent / "monkey" / "monkey_fleet.py"
ENGINE_ROOT = Path(upgrade.__file__).parents[upgrade.__name__.count(".")]
INSTANCES = Path.home() / ".tabsdata" / "instances"
SERVER_TIMEOUT = 1800
QUESTION = b"[y/yes/n/no]:"
ANSWER = b"y\n"
READ_CHUNK = 65536
PHASE_SHARE = 0.4
REPORTED_PROBLEMS = 30
TIMING_COLUMNS = (
    "Action",
    "Before",
    "p50",
    "p90",
    "p99",
    "After",
)
GLOBAL_ARGUMENTS = (
    "--accept-terms",
    "yes",
    "--verbose",
)
WINDOWS = sys.platform == "win32"
SECRET_VARIABLES = (
    "OPENAI__API_KEY",
    "ANTHROPIC__API_KEY",
)
REDACTION = "***"
LINE_BREAKS = re.compile(r"[\r\n]")
TERMINAL_CODES = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
HOP_STARTED = re.compile(r"Upgrading to (\S+)")
HOP_FINISHED = re.compile(r"Upgraded to (\S+) \((\d+) changed, (\d+) retained\)")


@dataclass(frozen=True)
class Source:
    version: str | None = None
    venv: Path | None = None

    @property
    def label(self) -> str:
        return f"from-{self.version}" if self.version else "from-venv"


def sources() -> list[Source]:
    versions = [version.strip() for version in FROM_VERSIONS.split(",")]
    listed = [Source(version=version) for version in versions if version]
    if FROM_VENV:
        listed.append(Source(venv=Path(FROM_VENV).expanduser()))
    return listed


SOURCES = sources()

pytestmark = [
    pytest.mark.sdk,
    pytest.mark.slow,
    pytest.mark.monkey,
    pytest.mark.xdist_group("monkey_seq"),
    pytest.mark.skipif(
        not ENTERPRISE,
        reason="monkey runs need downstream functions triggered as their inputs change",
    ),
    pytest.mark.skipif(
        not SOURCES,
        reason=(
            "upgrade runs need TD_MONKEY_UPGRADE_FROM_VERSIONS or "
            "TD_MONKEY_UPGRADE_FROM_VENV"
        ),
    ),
]


def setting(name: str) -> Path:
    value = os.environ.get(name)
    if not value:
        pytest.fail(
            f"{name} is not set; run upgrade runs through 'cargo make itest_py'.",
            pytrace=False,
        )
    return Path(value)


@pytest.fixture
def progress() -> Iterator[Progress]:
    reporter = Progress.open(PROGRESS)
    yield reporter
    reporter.close()


def environment(owner: PythonEnvironment, release_side: bool) -> dict[str, str]:
    path = f"{owner.scripts()}{os.pathsep}{os.environ.get('PATH', '')}"
    base = dict(
        os.environ,
        PATH=path,
        TDK_IN_AGENT="1",
    )
    if not release_side:
        return release.utf8_environment(base)
    variables = release.isolated_environment(base)
    variables.pop(UV_MODE, None)
    return variables


class LineFeed:
    def __init__(self, on_line: Callable[[str], None] | None):
        self.on_line = on_line
        self.pending = ""

    def feed(self, text: str) -> None:
        if self.on_line is None:
            return
        *complete, self.pending = LINE_BREAKS.split(self.pending + text)
        for line in complete:
            self.on_line(line)

    def close(self) -> None:
        if self.on_line is not None and self.pending:
            self.on_line(self.pending)
        self.pending = ""


class HopReport:
    def __init__(self, progress: Progress, instance: str):
        self.progress = progress
        self.instance = instance
        self.reported: set[tuple[str, str]] = set()

    def __call__(self, line: str) -> None:
        text = TERMINAL_CODES.sub("", line)
        if match := HOP_FINISHED.search(text):
            version, changed, retained = match.groups()
            self.report(
                ("finished", version),
                f"Upgraded {self.instance} to {version} "
                f"({changed} changed, {retained} retained)",
            )
        elif match := HOP_STARTED.search(text):
            version = match.group(1)
            self.report(("started", version), f"Upgrading {self.instance} to {version}")

    def report(self, key: tuple[str, str], message: str) -> None:
        if key not in self.reported:
            self.reported.add(key)
            self.progress(message, SETUP)


def in_terminal(
    argv: list[str],
    env: dict[str, str],
    on_line: Callable[[str], None] | None = None,
) -> tuple[int, str]:
    import pty

    controller, terminal = pty.openpty()
    process = subprocess.Popen(
        argv,
        env=env,
        stdin=terminal,
        stdout=terminal,
        stderr=terminal,
    )
    os.close(terminal)
    output = bytearray()
    decoder = codecs.getincrementaldecoder(release.ENCODING)(
        errors=release.DECODING_ERRORS,
    )
    lines = LineFeed(on_line)
    deadline = time.monotonic() + SERVER_TIMEOUT
    try:
        while time.monotonic() < deadline:
            if select.select([controller], [], [], 1)[0]:
                try:
                    chunk = os.read(controller, READ_CHUNK)
                except OSError:
                    break
                if not chunk:
                    break
                output += chunk
                lines.feed(decoder.decode(chunk))
                if output.rstrip().endswith(QUESTION):
                    os.write(controller, ANSWER)
            elif process.poll() is not None:
                break
        else:
            process.kill()
        lines.feed(decoder.decode(b"", final=True))
        lines.close()
        return (
            process.wait(),
            output.decode(release.ENCODING, errors=release.DECODING_ERRORS),
        )
    finally:
        os.close(controller)


def piped(
    argv: list[str],
    env: dict[str, str],
    on_line: Callable[[str], None] | None = None,
) -> tuple[int, str]:
    process = subprocess.Popen(
        argv,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding=release.ENCODING,
        errors=release.DECODING_ERRORS,
    )
    output = []
    lines = LineFeed(on_line)
    for line in process.stdout:
        output.append(line)
        lines.feed(line)
    lines.close()
    return process.wait(timeout=SERVER_TIMEOUT), "".join(output)


def redacted(text: str) -> str:
    for name in SECRET_VARIABLES:
        secret = os.environ.get(name)
        if secret:
            text = text.replace(secret, REDACTION)
    return text


def subcommand(arguments: list[str]) -> list[str]:
    return list(takewhile(lambda argument: not argument.startswith("-"), arguments))


def tdkserver(
    owner: PythonEnvironment,
    arguments: list[str],
    release_side: bool,
    extra: dict[str, str] | None = None,
    on_line: Callable[[str], None] | None = None,
):
    argv = [
        str(owner.binary("tdkserver")),
        *arguments,
    ]
    variables = dict(
        environment(owner, release_side),
        **(extra or {}),
    )
    code, output = (piped if WINDOWS else in_terminal)(argv, variables, on_line)
    if code:
        raise AssertionError(
            f"tdkserver {' '.join(subcommand(arguments))} failed ({code}): "
            f"{redacted(output)}",
        )


def command(name: str, instance: str) -> list[str]:
    return [
        name,
        "--instance",
        instance,
        *GLOBAL_ARGUMENTS,
        "--yes",
    ]


def run_before(
    directory: Path,
    folder: Path,
    owner: PythonEnvironment,
) -> dict:
    variables = dict(
        environment(owner, release_side=True),
        PYTHONPATH=str(ENGINE_ROOT),
    )
    result = subprocess.run(
        [
            str(owner.python()),
            "-m",
            upgrade.__name__,
            str(directory),
            str(folder),
        ],
        env=variables,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        encoding=release.ENCODING,
        errors=release.DECODING_ERRORS,
        timeout=TIMEOUT,
    )
    if result.returncode:
        raise AssertionError(
            "The run before the upgrade failed: "
            f"{redacted(result.stdout + result.stderr)}",
        )
    return upgrade.read_yaml(directory / upgrade.BEFORE_FILE)


def seconds(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f} s"


def timing_rows(before: list[dict], after: list) -> list[tuple[str, ...]]:
    earlier = upgrade.timings(before)
    later = upgrade.timings([outcome.to_dict() for outcome in after])
    rows = []
    for action in sorted(set(earlier) | set(later)):
        old, new = earlier.get(action, {}), later.get(action, {})
        rows.append(
            (
                action,
                str(old.get("count", 0)),
                *(
                    f"{seconds(old.get(rank))} -> {seconds(new.get(rank))}"
                    for rank in ("p50", "p90", "p99")
                ),
                str(new.get("count", 0)),
            ),
        )
    return rows


def release_environment(source: Source) -> PythonEnvironment:
    if source.venv is not None:
        return PythonEnvironment(root=source.venv)
    return release.create_environment(source.version, setting(VENVS_FOLDER))


@pytest.mark.timeout(TIMEOUT)
@pytest.mark.parametrize(
    "source",
    SOURCES,
    ids=[source.label for source in SOURCES],
)
def test_a_monkey_run_holds_across_an_upgrade(
    source: Source,
    tmp_path: Path,
    progress: Progress,
):
    if FROM_VERSIONS and FROM_VENV:
        pytest.fail(
            "You cannot set both TD_MONKEY_UPGRADE_FROM_VERSIONS and "
            "TD_MONKEY_UPGRADE_FROM_VENV.",
            pytrace=False,
        )
    rulebook = setting(LAUNCH_CONFIG)
    requirements = release.unpinned_requirements(
        setting(REQUIREMENTS),
        tmp_path / "requirements-drivers.txt",
    )
    lock = setting(PORTS_LOCK)
    registry = setting(REGISTRY_FILE)
    if PLAN:
        plan = load_plan(Path(PLAN))
    else:
        seed = int(SEED) if SEED else random.SystemRandom().randrange(2**32)
        plan = upgrade.without_streams(
            generate(
                seed,
                OPERATIONS,
                LANES,
                FAULTS,
                FLEET_SCALE,
                LONGEST_SLEEP_SECONDS,
            ),
        )
    first, second = upgrade.split(plan, SPLIT)
    namespace = run_namespace()
    project = f"monkey_{namespace}"
    progress.layout(spec.name for spec in plan.fleet.functions)
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
    current = PythonEnvironment()
    owner: PythonEnvironment | None = None
    owner_is_release = True
    name: str | None = None
    created: PythonEnvironment | None = None
    try:
        progress(f"Preparing the release of {source.label}", SETUP)
        owner = release_environment(source)
        if source.venv is None:
            created = owner
        version = release.installed_version(owner)
        if source.version is not None and version != source.version:
            raise AssertionError(
                f"The environment for release {source.version} holds tabsdata "
                f"{version}.",
            )
        name = release.instance_name()
        port = release.claim_base_port(lock)
        release.register(registry, name)
        folder = INSTANCES / name
        progress(f"Creating instance {name} on {version} at port {port}", SETUP)
        launch = release.launch_commands(
            rulebook,
            version,
            {
                "instance": name,
                "port": port,
                "requirements": requirements,
                "uv_index": release.RELEASE_INDEX,
                "environment": os.environ,
            },
        )
        for launch_command in launch:
            tdkserver(
                owner,
                launch_command.arguments,
                release_side=True,
                extra=launch_command.environment,
            )
        progress(f"Running {len(first.ops)} operations on {version}", SETUP)
        before = run_before(tmp_path, folder, owner)
        problems += [f"Before the upgrade: {problem}" for problem in before["problems"]]
        progress(f"Stopping {name} with work in flight", SETUP)
        try:
            tdkserver(owner, command("stop", name), release_side=True)
        except AssertionError as error:
            tolerated = release.tolerated_stop_errors(rulebook, version)
            if not any(code in str(error) for code in tolerated):
                raise
            progress(
                f"Release {version} did not fully stop {name}; the upgrade must "
                "stop what it left running",
                SETUP,
            )
        head = importlib.metadata.version("tabsdata")
        progress(f"Upgrading {name} from {version} to {head}", SETUP)
        tdkserver(
            current,
            command("upgrade", name),
            release_side=False,
            on_line=HopReport(progress, name),
        )
        progress(f"Starting {name} on {head}", SETUP)
        owner, owner_is_release = current, False
        tdkserver(current, command("start", name), release_side=False)
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
            check_adopted_runs=False,
        ).execute()
        problems += report.problems
        problems += upgrade.graph_problems(upgrade.connect(port, project), project)
        progress.table(
            TIMING_COLUMNS,
            timing_rows(before["outcomes"], report.outcomes),
        )
    except Exception as error:
        problems.append(f"Run aborted: {type(error).__name__}: {error}")
    finally:
        if not KEEP:
            if name is not None and owner is not None and (INSTANCES / name).exists():
                for action in ("stop", "trash"):
                    try:
                        tdkserver(owner, command(action, name), owner_is_release)
                    except Exception as error:
                        progress(f"Could not {action} {name}: {error}", FAILED)
            if name is not None:
                release.unregister(registry, name)
            if created is not None:
                shutil.rmtree(created.root, ignore_errors=True)
    if problems:
        shown = problems[:REPORTED_PROBLEMS]
        if len(problems) > REPORTED_PROBLEMS:
            shown.append(f"And {len(problems) - REPORTED_PROBLEMS} more problems")
        pytest.fail(
            "\n".join(
                [
                    f"Upgrade run {source.label} failed; plan: {full}",
                    *shown,
                ],
            ),
            pytrace=False,
        )
