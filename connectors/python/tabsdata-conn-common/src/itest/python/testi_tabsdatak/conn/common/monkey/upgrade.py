#
# Copyright 2026 Tabsdata Inc.
#

"""Upgrade runs: one monkey plan split in two around an upgrade of the
instance. The first part runs on a released tabsdata, from that release's own
Python and SDK (see ``main``), and leaves its project unsettled, with
executions in flight when the instance stops. The second part runs on the
current build after the upgrade, adopts the project and settles it, so the
checks of a monkey run cover executions that span the upgrade. Stream
publishers are left out: the release side sets up no Kafka broker.

Imports here stay within what released SDKs provide."""

from __future__ import annotations

import dataclasses
import json
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import yaml

from tabsdatak.cli import template
from tabsdatak.common._json_serde import dehydrate
from tabsdatak.conn.dev import DataGenSrcConn, TrashDestConn
from tabsdatak.sdk.tabsdata_server import TabsdataServer
from testi_tabsdatak.conn.common.cluster import Cluster
from testi_tabsdatak.conn.common.monkey.descriptor import load_plan
from testi_tabsdatak.conn.common.monkey.naming import collection_name
from testi_tabsdatak.conn.common.monkey.plan import (
    DATAGEN,
    STREAM_PUBLISHER,
    TRASH,
    Fleet,
    Plan,
)
from testi_tabsdatak.conn.common.monkey.progress import Progress
from testi_tabsdatak.conn.common.monkey.project import write_project
from testi_tabsdatak.conn.common.monkey.runner import MonkeyRun

PLAN_FILE = "plan.yaml"
SETUP_FILE = "setup.yaml"
BEFORE_FILE = "before.yaml"
SETTINGS_FILE = ".settings.json"
APISERVER_PORT = "apiserver_public_port"
HOST = "127.0.0.1"
SUPERUSER = ("admin", "tabsdata")
PERCENTILES = (50, 90, 99)


def without_streams(plan: Plan) -> Plan:
    dropped = {spec.name for spec in plan.fleet.of_kind(STREAM_PUBLISHER)}
    functions = tuple(spec for spec in plan.fleet.functions if spec.name not in dropped)
    used = {spec.collection for spec in functions}
    fleet = Fleet(
        collections=tuple(spec for spec in plan.fleet.collections if spec.name in used),
        functions=functions,
    )
    ops = tuple(op for op in plan.ops if op.function not in dropped)
    return dataclasses.replace(plan, fleet=fleet, ops=ops)


def split(plan: Plan, share: float) -> tuple[Plan, Plan]:
    """The operations before the upgrade and those after it."""
    cut = round(len(plan.ops) * share)
    return (
        dataclasses.replace(plan, ops=plan.ops[:cut]),
        dataclasses.replace(plan, ops=plan.ops[cut:]),
    )


def write_yaml(path: Path, data: dict) -> None:
    path.write_text(yaml.safe_dump(data, sort_keys=False, width=100))


def read_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def instance_settings(folder: Path) -> dict:
    return json.loads((folder / SETTINGS_FILE).read_text())


def apiserver_port(settings: dict) -> int:
    return next(
        int(entry["port"])
        for entry in settings["ports"]
        if entry.get("role") == APISERVER_PORT
    )


def connect(port: int, project: str | None = None) -> TabsdataServer:
    server = TabsdataServer(f"{HOST}:{port}", *SUPERUSER)
    if project is not None:
        server.connection.project = project
    return server


def connection_document(kind: str | None) -> str | None:
    conn = {DATAGEN: DataGenSrcConn, TRASH: TrashDestConn}.get(kind)
    if conn is None:
        return None
    data = conn().to_dict()
    dehydrated, _ = dehydrate(data, data)
    return template.to_yaml(dehydrated)


def set_up(
    server: TabsdataServer,
    plan: Plan,
    project: str,
    namespace: str,
    root: Path,
    fleet_module: Path,
):
    """Creates the project, its collections and every function of the fleet,
    and returns the fleet's module."""
    server.create_project(project, description="monkey upgrade run")
    server.connection.project = project
    for spec in plan.fleet.collections:
        server.create_collection(
            collection_name(namespace, spec.name),
            group=spec.group,
            connection=connection_document(spec.connection),
            secrets=[],
        )
    module = write_project(root, plan.fleet, namespace, fleet_module)
    for spec in plan.fleet.registration_order():
        server.get_collection(
            collection_name(namespace, spec.collection)
        ).register_function(
            info=getattr(module, spec.name),
            project_root=str(root),
            function_name=spec.name,
        )
    return module


def entity_id(project) -> str:
    """The project's stable id: ``project_id`` since projects became
    versioned, ``id`` in the releases before, which this half may run on."""
    return getattr(project, "project_id", None) or project.id


def before(directory: Path, instance_folder: Path) -> None:
    """The part before the upgrade, run by a release's Python. Reads the plan
    and the setup the test wrote, and writes what the lanes did."""
    plan = load_plan(directory / PLAN_FILE)
    setup = read_yaml(directory / SETUP_FILE)
    settings = instance_settings(instance_folder)
    port = apiserver_port(settings)
    root = directory / "project"
    module = set_up(
        connect(port),
        plan,
        setup["project"],
        setup["namespace"],
        root,
        Path(setup["fleet_module"]),
    )
    project = setup["project"]
    project_id = entity_id(connect(port).get_project(project))
    report = MonkeyRun(
        plan=plan,
        namespace=setup["namespace"],
        connect=lambda: connect(port, project),
        cluster=Cluster(SimpleNamespace(settings=settings)),
        project=project,
        project_id=project_id,
        project_root=root,
        module=module,
        progress=Progress.open(setup.get("progress")),
        deadline=time.monotonic() + setup["timeout"],
        settle=False,
    ).execute()
    write_yaml(
        directory / BEFORE_FILE,
        {
            "project_id": project_id,
            "duration": report.duration,
            "problems": report.problems,
            "outcomes": [outcome.to_dict() for outcome in report.outcomes],
        },
    )


def percentile(values: list[float], rank: int) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, -(-rank * len(ordered) // 100) - 1)]


def timings(outcomes: list[dict]) -> dict[str, dict]:
    """Per action, the spread of how long accepted operations took."""
    by_action: dict[str, list[float]] = {}
    for outcome in outcomes:
        if outcome.get("result") == "ok":
            by_action.setdefault(outcome["action"], []).append(outcome["elapsed"])
    return {
        action: {
            "count": len(values),
            **{f"p{rank}": percentile(values, rank) for rank in PERCENTILES},
        }
        for action, values in sorted(by_action.items())
    }


def listed(server: TabsdataServer, path: str) -> list[dict]:
    items, params = [], {}
    while True:
        response = server.connection.get(path, params=params)
        response.raise_for_status()
        page = response.json()["data"]
        items += page.get("data") or []
        if page.get("next") is None or page.get("next_pagination_id") is None:
            return items
        params = {"next": page["next"], "pagination_id": page["next_pagination_id"]}


def graph_problems(server: TabsdataServer, project: str) -> list[str]:
    """Every topology version of the project chains to the next one, and the
    project's graph reads at each of them."""
    versions = sorted(
        listed(server, f"/projects/{project}/topology-versions"),
        key=lambda version: version["valid_from"],
    )
    if not versions:
        return [f"Project {project} has no topology version"]
    problems = []
    for earlier, later in zip(versions, versions[1:]):
        if earlier["valid_to"] != later["valid_from"]:
            problems.append(
                f"Topology version {earlier['id']} ends at {earlier['valid_to']}, "
                f"but the next one, {later['id']}, starts at {later['valid_from']}"
            )
    if versions[-1]["valid_to"] is not None:
        problems.append(f"The latest topology version {versions[-1]['id']} is closed")
    for version in versions:
        response = server.connection.get(
            f"/projects/{project}/graph", params={"topology_version": version["id"]}
        )
        if response.status_code != 200:
            problems.append(
                f"The graph at topology version {version['id']} answered "
                f"{response.status_code}: {response.text[:300]}"
            )
        elif response.json()["data"]["version"] != version["id"]:
            problems.append(
                f"The graph read at topology version {version['id']} is of version "
                f"{response.json()['data']['version']}"
            )
    return problems


def main(argv: list[str]) -> None:
    directory, instance_folder = argv
    before(Path(directory), Path(instance_folder))


if __name__ == "__main__":
    main(sys.argv[1:])
