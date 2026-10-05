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

Every stored descriptor under ``monkey/descriptors`` is replayed first; then a
new random plan runs, and if it fails its descriptor is written there to be
committed. Each run works in its own project and namespace, so runs never see
each other's objects. Enterprise only: downstream functions must trigger as
their inputs change.

The defaults keep a run to about two minutes. Tuning: ``MONKEY_SEED``,
``MONKEY_OPS`` (60), ``MONKEY_LANES`` (4), ``MONKEY_SCALE`` (0.25, fleet size
factor), ``MONKEY_MAX_SECONDS`` (20, longest publisher sleep),
``MONKEY_FAULTS`` (1) and ``MONKEY_TIMEOUT`` (300 seconds, to raise with the
others); ``monkey.md`` in tabsdata-ee lists heavier profiles. Live
progress, off by default: ``MONKEY_PROGRESS=tty`` writes it to the terminal the
run was launched from, any other value is a file it is appended to.
"""

from __future__ import annotations

import os
import random
import time
from collections import Counter
from collections.abc import Iterator
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

OPERATIONS = int(os.environ.get("MONKEY_OPS", "60"))
LANES = int(os.environ.get("MONKEY_LANES", "4"))
SCALE = float(os.environ.get("MONKEY_SCALE", "0.25"))
MAX_SECONDS = int(os.environ.get("MONKEY_MAX_SECONDS", "20"))
FAULTS = os.environ.get("MONKEY_FAULTS", "1") != "0"
SEED = os.environ.get("MONKEY_SEED")
PROGRESS = os.environ.get("MONKEY_PROGRESS")

TIMEOUT = int(os.environ.get("MONKEY_TIMEOUT", "300"))
# Kept back from the run's own deadline, so a run out of time still reports
# its problems and writes its descriptor before pytest's timeout stops it.
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
    return variable if variable in os.environ else f"{variable} (default)"


def random_parameters(seed: int, plan: Plan) -> list[tuple[str, str, str]]:
    return [
        ("Seed", str(seed), "MONKEY_SEED" if SEED else "random"),
        ("Operations", str(OPERATIONS), setting("MONKEY_OPS")),
        ("Lanes", str(LANES), setting("MONKEY_LANES")),
        ("Fleet scale", str(SCALE), setting("MONKEY_SCALE")),
        ("Functions", fleet_text(plan), "fleet scale"),
        ("Longest publisher sleep", f"{MAX_SECONDS} s", setting("MONKEY_MAX_SECONDS")),
        ("Faults", "on" if FAULTS else "off", setting("MONKEY_FAULTS")),
        ("Timeout", spoken(TIMEOUT), setting("MONKEY_TIMEOUT")),
        ("Progress", PROGRESS or "off", "MONKEY_PROGRESS"),
    ]


def stored_parameters(descriptor: Path, plan: Plan) -> list[tuple[str, str, str]]:
    return [
        ("Descriptor", descriptor.name, "stored"),
        ("Seed", str(plan.seed), "descriptor"),
        ("Operations", str(len(plan.ops)), "descriptor"),
        ("Lanes", str(plan.lanes), "descriptor"),
        ("Functions", fleet_text(plan), "descriptor"),
        ("Longest publisher sleep", longest_sleep(plan), "descriptor"),
        ("Faults", "on" if plan.faults else "off", "descriptor"),
        ("Timeout", spoken(TIMEOUT), setting("MONKEY_TIMEOUT")),
        ("Progress", PROGRESS or "off", "MONKEY_PROGRESS"),
    ]


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
) -> tuple[str, Report]:
    deadline = time.monotonic() + TIMEOUT - REPORT_MARGIN_SECONDS
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
@pytest.mark.timeout(TIMEOUT)
@pytest.mark.parametrize("descriptor", stored(DESCRIPTORS), ids=lambda path: path.stem)
def test_a_stored_monkey_run_holds(
    descriptor: Path,
    instance_tdtest: Instance,
    domain_redpanda: RedpandaDomain,
    collection_factory: CollectionFactory,
    server: TabsdataServer,
    tmp_path: Path,
    progress: Progress,
):
    plan = load_plan(descriptor)
    progress.layout(len(plan.ops), (spec.name for spec in plan.fleet.functions))
    progress(f"Replaying {descriptor.name}", SETUP)
    progress.table(PARAMETERS, stored_parameters(descriptor, plan))
    _, report = run_plan(
        plan,
        instance_tdtest,
        domain_redpanda,
        collection_factory,
        server,
        tmp_path,
        progress,
    )
    assert not report.problems, summary(report, descriptor)


@tabsdata_instance(consumer="tdtest")
@redpanda_domain("redpanda")
@pytest.mark.timeout(TIMEOUT)
def test_a_random_monkey_run_holds(
    instance_tdtest: Instance,
    domain_redpanda: RedpandaDomain,
    collection_factory: CollectionFactory,
    server: TabsdataServer,
    tmp_path: Path,
    progress: Progress,
):
    seed = int(SEED) if SEED else random.SystemRandom().randrange(2**32)
    plan = generate(seed, OPERATIONS, LANES, FAULTS, SCALE, MAX_SECONDS)
    progress.layout(len(plan.ops), (spec.name for spec in plan.fleet.functions))
    progress("Starting a random run", SETUP)
    progress.table(PARAMETERS, random_parameters(seed, plan))
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
        )
    except Exception as error:
        report = Report([], [f"Run aborted: {type(error).__name__}: {error}"], {}, {})
        progress(report.problems[0], FAILED)
    if report.problems:
        descriptor = write_failure(DESCRIPTORS, plan, namespace, report)
        progress(f"Descriptor written to {descriptor}", FAILED)
        pytest.fail(summary(report, descriptor), pytrace=False)
