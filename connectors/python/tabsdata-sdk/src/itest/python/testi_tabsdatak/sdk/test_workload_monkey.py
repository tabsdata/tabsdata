#
# Copyright 2026 Tabsdata Inc.
#

"""Monkey runs on a cluster instance: a fleet of publishers, cron and stream
publishers, transformers and subscribers put through quasi-random operations
from several lanes at once (register, update, enable, disable, delete, trigger,
cancel and recover, plus lost jobs, lost streamers and orphans behind the
server's back), then checked for overlapping workloads, server errors and a
settled state that matches what the server accepted. It sits with the other
worker workload tests; Kafka (a Redpanda domain) is only the source its stream
publishers read.

A new random plan runs, and if it fails its descriptor is written under
``monkey/descriptors`` to be committed. With ``TD_MONKEY_SCENARIOS=1`` every
stored descriptor there is replayed first. Each run works in its own project
and namespace, so runs never see each other's objects. Enterprise only:
downstream functions must trigger as their inputs change.

``TD_MONKEY_WEIGHT`` (``--monkey-weight`` of the test tasks) picks a size:
``light`` (about 5 minutes), ``medium`` (about 15) or ``heavy`` (about 30, the
default); see ``WEIGHTS``. Each of ``TD_MONKEY_OPERATIONS``,
``TD_MONKEY_FLEET_SCALE``, ``TD_MONKEY_LANES``,
``TD_MONKEY_LONGEST_SLEEP_SECONDS``, ``TD_MONKEY_RUN_TIMEOUT_SECONDS`` (each
run) and ``TD_MONKEY_TOTAL_TIMEOUT_SECONDS`` (the replays and the random run
together) overrides its weight's value. Also ``TD_MONKEY_FAULTS`` (1),
``TD_MONKEY_SEED`` and ``TD_MONKEY_SCENARIOS`` (0). The developer guide in
tabsdata-ee lists other profiles, under Development Techniques, "Monkey Tests
for Execution Plans". Live progress, off by default:
``TD_MONKEY_PROGRESS=tty`` writes it to the terminal the run was launched from,
any other value is a file it is appended to.
"""

from __future__ import annotations

import os
import random
import time
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.redpanda import RedpandaDomain, redpanda_domain
from tabsdatax.itest.fixture.functions.harness import CollectionFactory
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance

from tabsdatak.conn.dev import DataGenSrcConn, TrashDestConn
from tabsdatak.extensions.variant.edition import ENTERPRISE
from tabsdatak.sdk.tabsdata_server import TabsdataServer
from testi_tabsdatak.conn.common.cluster import Cluster
from testi_tabsdatak.conn.common.monkey.descriptor import (
    load_plan,
    stored,
    write_failure,
    write_plan,
)
from testi_tabsdatak.conn.common.monkey.naming import (
    collection_name,
    run_namespace,
    topic_name,
)
from testi_tabsdatak.conn.common.monkey.plan import (
    CRON_PUBLISHER,
    DATAGEN,
    KAFKA,
    PUBLISHER,
    STREAM_PUBLISHER,
    SUBSCRIBER,
    TRANSFORMER,
    TRASH,
    Plan,
    generate,
)
from testi_tabsdatak.conn.common.monkey.progress import (
    FAILED,
    SETUP,
    Progress,
    counted,
    spoken,
)
from testi_tabsdatak.conn.common.monkey.project import write_project
from testi_tabsdatak.conn.common.monkey.runner import MonkeyRun, Report
from testi_tabsdatak.conn.kafka.backend import broker, src_conn

pytestmark = [
    pytest.mark.sdk,
    pytest.mark.slow,
    pytest.mark.monkey,
    pytest.mark.xdist_group("monkey_seq"),
    pytest.mark.skipif(
        not ENTERPRISE,
        reason="monkey runs need downstream functions triggered as their inputs change",
    ),
]

MONKEY = Path(__file__).parent / "monkey"
FLEET_MODULE = MONKEY / "monkey_fleet.py"
DESCRIPTORS = MONKEY / "descriptors"
PLAN_FILE = "monkey-plan.yaml"


@dataclass(frozen=True)
class Weight:
    operations: int
    fleet_scale: float
    lanes: int
    longest_sleep_seconds: int
    run_timeout_seconds: int
    total_timeout_seconds: int


# Calibrated against measured runs: about 15 operations per lane take two
# minutes, registration and the final settle included.
WEIGHTS = {
    "light": Weight(96, 0.5, 4, 20, 900, 1800),
    "medium": Weight(256, 1.0, 8, 30, 2700, 5400),
    "heavy": Weight(512, 2.0, 8, 20, 5400, 10800),
}
WEIGHT_NAME = os.environ.get("TD_MONKEY_WEIGHT") or "heavy"
if WEIGHT_NAME not in WEIGHTS:
    raise ValueError(
        f"TD_MONKEY_WEIGHT is {WEIGHT_NAME!r}; use one of {', '.join(WEIGHTS)}"
    )
WEIGHT = WEIGHTS[WEIGHT_NAME]


def tuned(variable: str, value: int | float, cast: Callable[[str], int | float]):
    text = os.environ.get(variable)
    return value if text is None else cast(text)


OPERATIONS = tuned("TD_MONKEY_OPERATIONS", WEIGHT.operations, int)
LANES = tuned("TD_MONKEY_LANES", WEIGHT.lanes, int)
FLEET_SCALE = tuned("TD_MONKEY_FLEET_SCALE", WEIGHT.fleet_scale, float)
LONGEST_SLEEP_SECONDS = tuned(
    "TD_MONKEY_LONGEST_SLEEP_SECONDS", WEIGHT.longest_sleep_seconds, int
)
RUN_TIMEOUT_SECONDS = tuned(
    "TD_MONKEY_RUN_TIMEOUT_SECONDS", WEIGHT.run_timeout_seconds, int
)
TOTAL_TIMEOUT_SECONDS = tuned(
    "TD_MONKEY_TOTAL_TIMEOUT_SECONDS", WEIGHT.total_timeout_seconds, int
)
FAULTS = os.environ.get("TD_MONKEY_FAULTS", "1") != "0"
SEED = os.environ.get("TD_MONKEY_SEED")
PROGRESS = os.environ.get("TD_MONKEY_PROGRESS")
SCENARIOS = os.environ.get("TD_MONKEY_SCENARIOS", "0") != "0"

# Kept back from the run's own deadline, so a run out of time still reports
# its problems and writes its descriptor before pytest's timeout stops it; both
# clocks start with the test body (func_only), not with the instance fixtures.
REPORT_MARGIN_SECONDS = 30
PARAMETERS = ("Parameter", "Value", "Set by")
KIND_NAMES = {
    PUBLISHER: "publisher",
    CRON_PUBLISHER: "cron publisher",
    STREAM_PUBLISHER: "stream publisher",
    TRANSFORMER: "transformer",
    SUBSCRIBER: "subscriber",
}
REPORTED_PROBLEMS = 30


@pytest.fixture
def progress() -> Iterator[Progress]:
    reporter = Progress.open(PROGRESS)
    yield reporter
    reporter.close()


def fleet_text(plan: Plan) -> str:
    counts = Counter(spec.kind for spec in plan.fleet.functions)
    kinds = ", ".join(
        counted(counts[kind], name) for kind, name in KIND_NAMES.items() if counts[kind]
    )
    failing = sum(spec.fails for spec in plan.fleet.functions)
    if failing:
        kinds += f"; {counted(failing, 'failing transformer')}"
    return f"{len(plan.fleet.functions)} ({kinds})"


def longest_sleep(plan: Plan) -> str:
    return f"{max(spec.seconds for spec in plan.fleet.of_kind(PUBLISHER))} s"


def setting(variable: str) -> str:
    return variable if variable in os.environ else f"{WEIGHT_NAME} weight"


def default(variable: str) -> str:
    return variable if variable in os.environ else f"{variable} (default)"


def timeout_parameters(seconds: float) -> list[tuple[str, str, str]]:
    return [
        ("Timeout", spoken(seconds), "run timeout, or what is left of the total"),
        (
            "Run timeout",
            spoken(RUN_TIMEOUT_SECONDS),
            setting("TD_MONKEY_RUN_TIMEOUT_SECONDS"),
        ),
        (
            "Total timeout",
            spoken(TOTAL_TIMEOUT_SECONDS),
            setting("TD_MONKEY_TOTAL_TIMEOUT_SECONDS"),
        ),
        ("Progress", PROGRESS or "off", "TD_MONKEY_PROGRESS"),
    ]


def random_parameters(
    seed: int, plan: Plan, seconds: float
) -> list[tuple[str, str, str]]:
    return [
        ("Seed", str(seed), "TD_MONKEY_SEED" if SEED else "random"),
        ("Weight", WEIGHT_NAME, default("TD_MONKEY_WEIGHT")),
        ("Operations", str(OPERATIONS), setting("TD_MONKEY_OPERATIONS")),
        ("Lanes", str(LANES), setting("TD_MONKEY_LANES")),
        ("Fleet scale", str(FLEET_SCALE), setting("TD_MONKEY_FLEET_SCALE")),
        ("Functions", fleet_text(plan), "fleet scale"),
        (
            "Longest publisher sleep",
            f"{LONGEST_SLEEP_SECONDS} s",
            setting("TD_MONKEY_LONGEST_SLEEP_SECONDS"),
        ),
        ("Faults", "on" if FAULTS else "off", default("TD_MONKEY_FAULTS")),
        *timeout_parameters(seconds),
    ]


def stored_parameters(
    descriptor: Path, plan: Plan, seconds: float
) -> list[tuple[str, str, str]]:
    return [
        ("Descriptor", descriptor.name, "stored"),
        ("Weight", WEIGHT_NAME, default("TD_MONKEY_WEIGHT") + ", timeouts only"),
        ("Seed", str(plan.seed), "descriptor"),
        ("Operations", str(len(plan.ops)), "descriptor"),
        ("Lanes", str(plan.lanes), "descriptor"),
        ("Functions", fleet_text(plan), "descriptor"),
        ("Longest publisher sleep", longest_sleep(plan), "descriptor"),
        ("Faults", "on" if plan.faults else "off", "descriptor"),
        *timeout_parameters(seconds),
    ]


@pytest.fixture(scope="module")
def total_deadline() -> float:
    return time.monotonic() + TOTAL_TIMEOUT_SECONDS


def run_seconds(total_deadline: float) -> float:
    left = total_deadline - time.monotonic()
    if left <= REPORT_MARGIN_SECONDS:
        pytest.fail(
            f"The monkey total timeout of {spoken(TOTAL_TIMEOUT_SECONDS)} "
            "(TD_MONKEY_TOTAL_TIMEOUT_SECONDS) was spent before this run started",
            pytrace=False,
        )
    return min(RUN_TIMEOUT_SECONDS, left)


def connect(instance: Instance, project: str) -> TabsdataServer:
    api = instance.apiserver
    server = TabsdataServer(
        f"{api.host}:{api.port}", api.superuser.user, api.superuser.password
    )
    server.connection.project = project
    return server


def connection(kind: str | None, domain: RedpandaDomain):
    if kind == DATAGEN:
        return DataGenSrcConn()
    if kind == TRASH:
        return TrashDestConn()
    if kind == KAFKA:
        return src_conn(domain)
    return None


def run_plan(
    plan: Plan,
    instance: Instance,
    domain: RedpandaDomain,
    factory: CollectionFactory,
    server: TabsdataServer,
    tmp_path: Path,
    progress: Progress,
    seconds: float,
) -> tuple[str, Report]:
    deadline = time.monotonic() + seconds - REPORT_MARGIN_SECONDS
    namespace = run_namespace()
    progress.layout(len(plan.ops), (spec.name for spec in plan.fleet.functions))
    progress(f"Plan written to {write_plan(tmp_path / PLAN_FILE, plan)}", SETUP)
    for spec in plan.fleet.collections:
        factory(
            spec.group,
            connection(spec.connection, domain),
            name=collection_name(namespace, spec.name),
        )
    project = server.connection.project
    project_id = server.get_project(project).id
    progress(f"Namespace {namespace} in project {project} ({project_id})", SETUP)
    kafka = broker(domain)
    topics = [
        topic_name(namespace, spec.name)
        for spec in plan.fleet.of_kind(STREAM_PUBLISHER)
    ]
    for topic in topics:
        kafka.create_topic(topic)
    try:
        root = tmp_path / "project"
        module = write_project(root, plan.fleet, namespace, FLEET_MODULE)
        progress(f"Registering {len(plan.fleet.functions)} functions", SETUP)
        for spec in plan.fleet.registration_order():
            server.get_collection(
                collection_name(namespace, spec.collection)
            ).register_function(
                info=getattr(module, spec.name),
                project_root=str(root),
                function_name=spec.name,
            )
        run = MonkeyRun(
            plan=plan,
            namespace=namespace,
            connect=lambda: connect(instance, project),
            cluster=Cluster(instance),
            project=project,
            project_id=project_id,
            project_root=root,
            module=module,
            progress=progress,
            deadline=deadline,
        )
        return namespace, run.execute()
    finally:
        for topic in topics:
            kafka.delete_topic(topic)


def summary(report: Report, descriptor: Path) -> str:
    lines = report.problems[:REPORTED_PROBLEMS]
    if len(report.problems) > REPORTED_PROBLEMS:
        lines.append(f"And {len(report.problems) - REPORTED_PROBLEMS} more problems")
    header = (
        f"Monkey run failed after {spoken(report.duration)}; descriptor: {descriptor}"
    )
    return "\n".join([header, *lines])


@tabsdata_instance(consumer="tdtest")
@redpanda_domain("redpanda")
@pytest.mark.timeout(RUN_TIMEOUT_SECONDS, func_only=True)
@pytest.mark.skipif(
    not SCENARIOS, reason="stored scenarios replay only with TD_MONKEY_SCENARIOS=1"
)
@pytest.mark.parametrize("descriptor", stored(DESCRIPTORS), ids=lambda path: path.stem)
def test_a_stored_monkey_run_holds(
    descriptor: Path,
    instance_tdtest: Instance,
    domain_redpanda: RedpandaDomain,
    collection_factory: CollectionFactory,
    server: TabsdataServer,
    tmp_path: Path,
    progress: Progress,
    total_deadline: float,
):
    seconds = run_seconds(total_deadline)
    plan = load_plan(descriptor)
    progress.layout(len(plan.ops), (spec.name for spec in plan.fleet.functions))
    progress(f"Replaying {descriptor.name}", SETUP)
    progress.table(PARAMETERS, stored_parameters(descriptor, plan, seconds))
    _, report = run_plan(
        plan,
        instance_tdtest,
        domain_redpanda,
        collection_factory,
        server,
        tmp_path,
        progress,
        seconds,
    )
    assert not report.problems, summary(report, descriptor)


@tabsdata_instance(consumer="tdtest")
@redpanda_domain("redpanda")
@pytest.mark.timeout(RUN_TIMEOUT_SECONDS, func_only=True)
def test_a_random_monkey_run_holds(
    instance_tdtest: Instance,
    domain_redpanda: RedpandaDomain,
    collection_factory: CollectionFactory,
    server: TabsdataServer,
    tmp_path: Path,
    progress: Progress,
    total_deadline: float,
):
    seconds = run_seconds(total_deadline)
    seed = int(SEED) if SEED else random.SystemRandom().randrange(2**32)
    plan = generate(seed, OPERATIONS, LANES, FAULTS, FLEET_SCALE, LONGEST_SLEEP_SECONDS)
    progress.layout(len(plan.ops), (spec.name for spec in plan.fleet.functions))
    progress("Starting a random run", SETUP)
    progress.table(PARAMETERS, random_parameters(seed, plan, seconds))
    namespace = "-"
    try:
        namespace, report = run_plan(
            plan,
            instance_tdtest,
            domain_redpanda,
            collection_factory,
            server,
            tmp_path,
            progress,
            seconds,
        )
    except Exception as error:
        report = Report([], [f"Run aborted: {type(error).__name__}: {error}"], {}, {})
        progress(report.problems[0], FAILED)
    if report.problems:
        descriptor = write_failure(DESCRIPTORS, plan, namespace, report)
        progress(f"Descriptor written to {descriptor}", FAILED)
        pytest.fail(summary(report, descriptor), pytrace=False)
