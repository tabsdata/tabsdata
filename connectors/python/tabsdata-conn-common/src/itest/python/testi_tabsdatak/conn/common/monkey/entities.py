#
# Copyright 2026 Tabsdata Inc.
#

"""The entity monkey: groups, collections and publishers of one project put
through random creates, renames, deletes and runs from several lanes at once,
against a model of what the server should hold afterwards. Each lane owns a
name prefix, so lanes never contend for a name, except for the shared names
every lane races to create. A collection that ever held a function keeps graph
rows (its tables outlive the function), so deleting it opens a topology
version; nothing else a lane does to groups, collections or projects moves the
topology, which the settle checks by counting versions."""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field

from tabsdatak.sdk._apiserver import APIServerError
from tabsdatak.sdk.status_utils.execution import (
    EXECUTION_FINAL_STATUSES,
    execution_status_to_mapping,
)
from tabsdatak.sdk.tabsdata_server import Collection, TabsdataServer
from testi_tabsdatak.conn.common.monkey.listing import listed

SOURCES_GROUP = "sources"
SYSTEM_GROUPS = ("default", "sources", "destinations")
SHARED_NAMES = 3
RUN_TIMEOUT = 600
RUN_POLL = 5

FALLBACK_ACTIONS = ("group_create", "source_create", "function_register")
ACTIONS = {
    "group_create": 8,
    "group_rename": 5,
    "group_delete": 6,
    "group_delete_system": 1,
    "group_race": 2,
    "collection_create": 10,
    "collection_rename": 5,
    "collection_describe": 3,
    "collection_delete": 7,
    "source_create": 6,
    "function_register": 8,
    "function_delete": 4,
    "function_trigger": 5,
}


@dataclass
class GroupState:
    name: str
    group_id: str


@dataclass
class CollectionState:
    name: str
    collection_id: str
    group_id: str
    source: bool
    function_version: str | None = None
    had_function: bool = False


@dataclass(frozen=True)
class Run:
    execution_id: str
    function_version: str
    collection_id: str
    topology_version: str


@dataclass
class Model:
    """What the server should hold: live groups and collections by name, the
    collections deleted while holding runs, the topology versions opened, and
    every execution with the version it planned on."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    groups: dict[str, GroupState] = field(default_factory=dict)
    collections: dict[str, CollectionState] = field(default_factory=dict)
    retired_groups: list[GroupState] = field(default_factory=list)
    retired_collections: list[CollectionState] = field(default_factory=list)
    topology_versions: int = 1
    runs: list[Run] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)
    shared: set[str] = field(default_factory=set)
    counter: int = 0

    def next_name(self, lane: int, kind: str) -> str:
        with self.lock:
            self.counter += 1
            return f"{kind}{lane}_{self.counter}"

    def problem(self, text: str) -> None:
        with self.lock:
            self.problems.append(text)


def deleted_label(collection_id: str) -> str:
    return f"[{collection_id}]"


def refused(error: Exception, *codes: str) -> bool:
    code = getattr(error, "code", None) or ""
    return any(code.startswith(prefix) for prefix in codes)


class Lane:
    """One lane of operations over the entities whose names carry its index."""

    def __init__(
        self,
        index: int,
        rng: random.Random,
        server: TabsdataServer,
        project: str,
        model: Model,
        source_document: str,
        publisher,
        publisher_root: str,
    ):
        self.index = index
        self.rng = rng
        self.server = server
        self.project = project
        self.model = model
        self.source_document = source_document
        self.publisher = publisher
        self.publisher_root = publisher_root

    def mine(self, states: dict) -> list:
        with self.model.lock:
            return [state for name, state in states.items() if self.owns(name)]

    def owns(self, name: str) -> bool:
        prefix = name.split("_", 1)[0]
        return prefix in (f"g{self.index}", f"c{self.index}", f"s{self.index}")

    def pick(self, candidates: list):
        return self.rng.choice(candidates) if candidates else None

    def step(self) -> tuple[str, str, str]:
        action = self.rng.choices(list(ACTIONS), weights=list(ACTIONS.values()))[0]
        try:
            outcome = getattr(self, action)()
            if outcome is None:
                action = self.rng.choice(FALLBACK_ACTIONS)
                outcome = getattr(self, action)()
        except APIServerError as error:
            self.model.problem(f"{action}: {error.code} {error}")
            return action, "error", f"{error.code} {error}"
        except Exception as error:
            self.model.problem(f"{action}: {type(error).__name__}: {error}")
            return action, "error", f"{type(error).__name__}: {error}"
        if outcome is None:
            return action, "skipped", "nothing to act on"
        return action, "ok", outcome

    def group_create(self) -> str:
        with self.model.lock:
            retired = [g for g in self.model.retired_groups if self.owns(g.name)]
        old = self.pick(retired) if retired and self.rng.random() < 0.5 else None
        name = old.name if old else self.model.next_name(self.index, "g")
        group = self.server.create_group(name, description=f"lane {self.index}")
        with self.model.lock:
            if old is not None:
                self.model.retired_groups.remove(old)
                if group.group_id == old.group_id:
                    self.model.problems.append(
                        f"group {name} recreated with its old id {old.group_id}"
                    )
            self.model.groups[name] = GroupState(name, group.group_id)
        return f"{name} as {group.group_id}" + (" (name reused)" if old else "")

    def group_rename(self) -> str | None:
        state = self.pick(self.mine(self.model.groups))
        if state is None:
            return None
        new_name = self.model.next_name(self.index, "g")
        self.server.update_group(state.name, new_name=new_name)
        with self.model.lock:
            del self.model.groups[state.name]
            state.name = new_name
            self.model.groups[new_name] = state
        return f"{new_name} keeps {state.group_id}"

    def group_delete(self) -> str | None:
        state = self.pick(self.mine(self.model.groups))
        if state is None:
            return None
        with self.model.lock:
            live = [
                c.name
                for c in self.model.collections.values()
                if c.group_id == state.group_id
            ]
        try:
            self.server.delete_group(state.name)
        except APIServerError as error:
            if live and refused(error, "GroupError"):
                return f"{state.name} refused, holds {len(live)}"
            raise
        if live:
            self.model.problem(f"group {state.name} deleted while holding {live}")
        with self.model.lock:
            del self.model.groups[state.name]
            self.model.retired_groups.append(state)
        return f"{state.name} gone"

    def group_delete_system(self) -> str:
        name = self.rng.choice(SYSTEM_GROUPS)
        try:
            self.server.delete_group(name)
        except APIServerError as error:
            if refused(error, "GroupError"):
                return f"{name} refused"
            raise
        self.model.problem(f"system group {name} deleted")
        return f"{name} deleted"

    def group_race(self) -> str:
        name = f"shared_{self.rng.randrange(SHARED_NAMES)}"
        try:
            group = self.server.create_group(name, description="shared")
        except APIServerError as error:
            with self.model.lock:
                known = name in self.model.shared
            if known and "already exists" in str(error):
                return f"{name} lost"
            raise
        with self.model.lock:
            if name in self.model.shared:
                self.model.problems.append(f"shared group {name} created twice")
            self.model.shared.add(name)
            self.model.groups[name] = GroupState(name, group.group_id)
        return f"{name} won"

    def group_for_collection(self) -> GroupState | None:
        return self.pick(self.mine(self.model.groups))

    def create_collection(self, source: bool) -> str | None:
        if source:
            group_name, group_id = SOURCES_GROUP, self.sources_group_id()
        else:
            group = self.group_for_collection()
            if group is None:
                return None
            group_name, group_id = group.name, group.group_id
        kind = "s" if source else "c"
        with self.model.lock:
            retired = [c for c in self.model.retired_collections if self.owns(c.name)]
        old = self.pick(retired) if retired and self.rng.random() < 0.5 else None
        name = old.name if old else self.model.next_name(self.index, kind)
        collection = Collection(
            self.server.connection,
            name,
            description=f"lane {self.index}",
            group=group_name,
            connection_document=self.source_document if source else None,
        )
        collection.create()
        collection_id = collection._data["collection_id"]
        with self.model.lock:
            if old is not None:
                self.model.retired_collections.remove(old)
                if collection_id == old.collection_id:
                    self.model.problems.append(
                        f"collection {name} recreated with its old id"
                    )
            self.model.collections[name] = CollectionState(
                name, collection_id, group_id, source
            )
        return f"{name} in {group_name}" + (" (name reused)" if old else "")

    def sources_group_id(self) -> str:
        with self.model.lock:
            cached = self.model.groups.get(SOURCES_GROUP)
        if cached is not None:
            return cached.group_id
        group = self.server.get_group(SOURCES_GROUP)
        with self.model.lock:
            self.model.groups.setdefault(
                SOURCES_GROUP, GroupState(SOURCES_GROUP, group.group_id)
            )
        return group.group_id

    def collection_create(self) -> str | None:
        return self.create_collection(source=False)

    def source_create(self) -> str | None:
        return self.create_collection(source=True)

    def collection_rename(self) -> str | None:
        state = self.pick(self.mine(self.model.collections))
        if state is None:
            return None
        new_name = self.model.next_name(self.index, "s" if state.source else "c")
        self.server.update_collection(state.name, new_name=new_name)
        with self.model.lock:
            del self.model.collections[state.name]
            state.name = new_name
            self.model.collections[new_name] = state
        return f"{new_name} keeps {state.collection_id}"

    def collection_describe(self) -> str | None:
        state = self.pick(self.mine(self.model.collections))
        if state is None:
            return None
        self.server.update_collection(
            state.name, new_description=f"touched {time.time():.0f}"
        )
        return state.name

    def collection_delete(self) -> str | None:
        state = self.pick(self.mine(self.model.collections))
        if state is None:
            return None
        self.server.delete_collection(state.name)
        with self.model.lock:
            del self.model.collections[state.name]
            self.model.retired_collections.append(state)
            if state.had_function:
                self.model.topology_versions += 1
        return f"{state.name}" + (" (graph changed)" if state.had_function else "")

    def function_register(self) -> str | None:
        state = self.pick([
            c
            for c in self.mine(self.model.collections)
            if c.source and not c.function_version
        ])
        if state is None:
            return None
        collection = Collection(self.server.connection, state.name)
        function = collection.register_function(
            info=self.publisher,
            project_root=self.publisher_root,
            function_name="quick",
            reuse_tables=True,
        )
        with self.model.lock:
            state.function_version = function.id
            state.had_function = True
            self.model.topology_versions += 1
        return f"quick in {state.name} as {function.id}"

    def function_delete(self) -> str | None:
        state = self.pick([
            c for c in self.mine(self.model.collections) if c.function_version
        ])
        if state is None:
            return None
        Collection(self.server.connection, state.name).get_function("quick").delete()
        with self.model.lock:
            state.function_version = None
            self.model.topology_versions += 1
        return f"quick in {state.name}"

    def function_trigger(self) -> str | None:
        state = self.pick([
            c for c in self.mine(self.model.collections) if c.function_version
        ])
        if state is None:
            return None
        function = Collection(self.server.connection, state.name).get_function("quick")
        execution = function.trigger()
        deadline = time.monotonic() + RUN_TIMEOUT
        while True:
            data = self.server.connection.execution_list(
                request_filter=f"id:eq:{execution.id}"
            ).json()["data"]["data"][0]
            status = execution_status_to_mapping(data["status"])
            if status in EXECUTION_FINAL_STATUSES:
                break
            if time.monotonic() > deadline:
                self.model.problem(f"execution {execution.id} never finished")
                break
            time.sleep(RUN_POLL)
        run = Run(
            execution.id,
            data["function_version_id"],
            state.collection_id,
            data["topology_version_id"],
        )
        with self.model.lock:
            self.model.runs.append(run)
        return f"{execution.id} {status} on {run.topology_version}"


def settle(server: TabsdataServer, project: str, model: Model) -> list[str]:
    """Compare the server with the model: live groups and collections, the
    count of topology versions, and the labels every run and every pinned
    graph show for live and deleted collections."""
    problems: list[str] = list(model.problems)
    groups = {g.name: g.group_id for g in server.list_groups() if not g.system}
    expected_groups = {
        name: state.group_id
        for name, state in model.groups.items()
        if name not in SYSTEM_GROUPS
    }
    if groups != expected_groups:
        problems.append(f"groups differ: server {groups} model {expected_groups}")
    collections = {
        c.name: (c.collection_id, c.kwargs.get("group_id"))
        for c in server.list_collections()
    }
    expected_collections = {
        name: (state.collection_id, state.group_id)
        for name, state in model.collections.items()
    }
    if collections != expected_collections:
        problems.append(
            f"collections differ: server {collections} model {expected_collections}"
        )
    found = len(listed(server, f"/projects/{project}/topology-versions"))
    if found != model.topology_versions:
        problems.append(
            f"topology versions: server {found}, model {model.topology_versions}"
        )
    live_by_id = {
        state.collection_id: name for name, state in model.collections.items()
    }
    for run in model.runs:
        label = live_by_id.get(run.collection_id, deleted_label(run.collection_id))
        deleted = run.collection_id not in live_by_id
        function_runs = server.connection.function_list_runs(
            request_filter=f"execution_id:eq:{run.execution_id}"
        ).json()["data"]["data"]
        if not function_runs:
            problems.append(f"execution {run.execution_id} has no function runs")
        for data in function_runs:
            if (
                data["collection"] != label
                or bool(data["collection_deleted"]) != deleted
            ):
                problems.append(
                    f"run {data['id']} shows {data['collection']!r} "
                    f"deleted={data['collection_deleted']}, expected "
                    f"{label!r} deleted={deleted}"
                )
        graph = server.connection.get(
            f"/projects/{project}/graph",
            params={"topology_version": run.topology_version},
        ).json()["data"]
        function = graph["all_functions"].get(run.function_version)
        if function is None:
            problems.append(
                f"graph at {run.topology_version} lacks {run.function_version}"
            )
        elif function["collection"] != label:
            problems.append(
                f"graph at {run.topology_version} labels {run.function_version} "
                f"{function['collection']!r}, expected {label!r}"
            )
    return problems
