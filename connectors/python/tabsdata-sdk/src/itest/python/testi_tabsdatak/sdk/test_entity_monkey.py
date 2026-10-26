#
# Copyright 2026 Tabsdata Inc.
#

"""A monkey run over the entities of one project: groups, collections and
publishers created, renamed, deleted and run from several lanes at once, then
settled against a model (see ``monkey.entities``). It checks the shape the
server keeps for them: live names map to stable ids, a deleted name recreates
with a new id, a group refuses to go while a live collection points at it and
goes once none does, runs of a deleted collection read `[id]`, the graph at
every pinned topology version resolves the same labels, and only graph writes
open topology versions.

Settings: ``TD_MONKEY_OPERATIONS`` (96), ``TD_MONKEY_LANES`` (4),
``TD_MONKEY_SEED``, ``TD_MONKEY_RUN_TIMEOUT_SECONDS`` (900) and
``TD_MONKEY_PROGRESS`` (as the workload monkey)."""

from __future__ import annotations

import os
import random
import threading
import time
from pathlib import Path

import pytest
from tabsdatax.itest.fixture.functions.harness import CollectionFactory
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance

from tabsdatak.conn.dev import DataGenSrcConn
from tabsdatak.sdk.tabsdata_server import TabsdataServer
from testi_tabsdatak.conn.common.function_project import function_module
from testi_tabsdatak.conn.common.monkey.entities import (
    SYSTEM_GROUPS,
    CollectionState,
    Lane,
    Model,
    settle,
)
from testi_tabsdatak.conn.common.monkey.progress import (
    FAILED,
    PASSED,
    SETTLE,
    SETUP,
    Progress,
    counted,
    spoken,
)

OPERATIONS = int(os.environ.get("TD_MONKEY_OPERATIONS", "96"))
LANES = int(os.environ.get("TD_MONKEY_LANES", "4"))
SEED = os.environ.get("TD_MONKEY_SEED")
RUN_TIMEOUT_SECONDS = int(os.environ.get("TD_MONKEY_RUN_TIMEOUT_SECONDS", "900"))
PROGRESS = os.environ.get("TD_MONKEY_PROGRESS")
REPORTED_PROBLEMS = 30
FUNCTIONS = Path(__file__).parent / "functions"
lifecycle = function_module(FUNCTIONS, "lifecycle")

pytestmark = [
    pytest.mark.sdk,
    pytest.mark.slow,
    pytest.mark.monkey,
    pytest.mark.xdist_group("monkey_seq"),
]


@pytest.fixture
def progress():
    reporter = Progress.open(PROGRESS)
    yield reporter
    reporter.close()


def run_lanes(lanes: list[Lane], operations: int, progress: Progress) -> None:
    done = 0
    done_lock = threading.Lock()
    share = [operations // len(lanes)] * len(lanes)
    share[0] += operations - sum(share)

    def drive(lane: Lane, count: int) -> None:
        nonlocal done
        for index in range(count):
            started = time.monotonic()
            action, result, detail = lane.step()
            with done_lock:
                done += 1
                progress.operation(
                    done,
                    operations,
                    lane.index,
                    index,
                    action,
                    None,
                    result,
                    None,
                    time.monotonic() - started,
                    detail,
                )

    threads = [
        threading.Thread(target=drive, args=(lane, count), daemon=True)
        for lane, count in zip(lanes, share)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def clean_up(server: TabsdataServer, model: Model) -> None:
    for name in list(model.collections):
        server.delete_collection(name, raise_for_status=False)
    for name in list(model.groups):
        if name not in SYSTEM_GROUPS:
            server.delete_group(name, raise_for_status=False)


@tabsdata_instance(consumer="tdtest")
@pytest.mark.timeout(RUN_TIMEOUT_SECONDS, func_only=True)
def test_entities_hold_under_a_monkey_run(
    instance_tdtest: Instance,
    collection_factory: CollectionFactory,
    server: TabsdataServer,
    progress: Progress,
):
    seed = int(SEED) if SEED else random.SystemRandom().randrange(2**32)
    rng = random.Random(seed)
    seeded = collection_factory("sources", DataGenSrcConn(), name="seed_source")
    project = server.connection.project
    document = seeded._connection_document
    model = Model()
    model.collections[seeded.name] = CollectionState(
        seeded.name,
        seeded._data["collection_id"],
        server.get_group("sources").group_id,
        source=True,
    )
    lanes = [
        Lane(
            index,
            random.Random(rng.randrange(2**32)),
            server,
            project,
            model,
            document,
            lifecycle.quick,
            str(FUNCTIONS),
        )
        for index in range(LANES)
    ]
    progress.layout(["-"])
    progress(
        f"Entity run seed {seed}: {counted(OPERATIONS, 'operation')} "
        f"on {counted(LANES, 'lane')} in {project}",
        SETUP,
    )
    started = time.monotonic()
    try:
        run_lanes(lanes, OPERATIONS, progress)
        progress("Settling", SETTLE)
        problems = settle(server, project, model)
    finally:
        clean_up(server, model)
    if problems:
        progress(
            f"{counted(len(problems), 'problem')} after "
            f"{spoken(time.monotonic() - started)}",
            FAILED,
        )
        shown = problems[:REPORTED_PROBLEMS]
        if len(problems) > REPORTED_PROBLEMS:
            shown.append(f"And {len(problems) - REPORTED_PROBLEMS} more problems")
        pytest.fail(
            "\n".join([f"Entity run (seed {seed}) failed:", *shown]), pytrace=False
        )
    progress(f"Settled after {spoken(time.monotonic() - started)}", PASSED)
