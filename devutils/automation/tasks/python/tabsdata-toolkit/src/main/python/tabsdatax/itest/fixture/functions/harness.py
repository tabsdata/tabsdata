#
# Copyright 2026 Tabsdata Inc.
#

"""Connector-agnostic end-to-end test harness: register real functions on a
running tabsdata instance, trigger them, and read results back.

Loaded as a pytest plugin (entry point ``tdf-functions``), so the ``server`` and
``collection_factory`` fixtures are available to any connector's itest without a
per-package conftest. The plain helpers (``run_function``, ``wait_for_completion``,
``assert_frames_match``) are importable from here.

A test declares ``@tabsdata_instance(consumer=...)`` (for ``server``) and, via
``collection_factory``, creates source/destination collections carrying any
connector's ``Conn``; file-specific helpers live in :mod:`.files`.
"""

from __future__ import annotations

import tempfile
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from time import sleep

import polars as pl
import pytest
from tabsdatak.sdk.status_utils.execution import (
    EXECUTION_FINAL_STATUSES,
    EXECUTION_SUCCESSFUL_FINAL_STATUSES,
)
from tabsdatak.sdk.tabsdata_server import Collection, FunctionInfo, TabsdataServer
from tabsdatak.spi import Conn

from tabsdatax.itest.fixture.instances.tabsdata import Instance

POLL_INTERVAL_SECONDS = 2
POLL_TIMEOUT_SECONDS = 300

# Creates a collection in a given group ("sources" / "destinations") with the
# given connection -- named explicitly, or generated when the name does not
# matter -- and returns the created ``Collection``.
CollectionFactory = Callable[..., Collection]


def wait_for_completion(execution) -> str:
    """Poll ``execution`` until it reaches a terminal state; return that status."""
    waited = 0
    while execution.status not in EXECUTION_FINAL_STATUSES:
        if waited >= POLL_TIMEOUT_SECONDS:
            pytest.fail(
                f"execution did not finish within {POLL_TIMEOUT_SECONDS}s "
                f"(last status {execution.status!r})",
                pytrace=False,
            )
        sleep(POLL_INTERVAL_SECONDS)
        waited += POLL_INTERVAL_SECONDS
        execution.refresh()
    return execution.status


def run_function(
    collection: Collection, function_info: FunctionInfo, project_root: Path
) -> None:
    """Register ``function_info`` in ``collection``, trigger it, assert success."""
    function = collection.register_function(
        info=function_info, project_root=str(project_root)
    )
    status = wait_for_completion(function.trigger())
    assert status in EXECUTION_SUCCESSFUL_FINAL_STATUSES, (
        f"execution finished with non-success status {status!r}"
    )


def table_frame(collection: Collection, table: str) -> pl.DataFrame:
    """Everything ``collection``'s `table` holds at HEAD. ``Table.sample`` is what
    its name says (an ``offset`` / ``len`` window), so a test asserting on a
    published table downloads it instead."""
    handle = collection.get_table(table)
    assert handle is not None, f"table {table!r} not found in {collection.name!r}"
    with tempfile.TemporaryDirectory() as tmp:
        parquet = Path(tmp) / f"{table}.parquet"
        handle.download(str(parquet))
        return pl.read_parquet(parquet)


def assert_frames_match(got: pl.DataFrame, expected: pl.DataFrame) -> None:
    """Order- and dtype-agnostic content equality on ``expected``'s columns
    (transporter format inference may differ from polars' own read)."""
    columns = expected.columns
    assert (
        got.select(columns).cast(pl.String).sort(by=columns).to_dicts()
        == expected.cast(pl.String).sort(by=columns).to_dicts()
    )


@pytest.fixture
def server(instance_tdtest: Instance) -> TabsdataServer:
    """A ``TabsdataServer`` client against the registered ``tdtest`` instance.

    Requires ``@tabsdata_instance(consumer="tdtest")`` on the test.
    """
    api = instance_tdtest.apiserver
    return TabsdataServer(
        f"{api.host}:{api.port}", api.superuser.user, api.superuser.password
    )


@pytest.fixture
def collection_factory(server: TabsdataServer) -> Iterator[CollectionFactory]:
    """Create collections under a per-test project, cleaning up every collection
    and the project on teardown. The project is created lazily on first use so a
    skipped test provisions nothing."""
    state: dict[str, str | None] = {"project": None}
    created: list[str] = []

    def make(group: str, conn: Conn, name: str | None = None) -> Collection:
        if state["project"] is None:
            project = f"e2e_{uuid.uuid4().hex[:8]}_proj"
            server.create_project(project, description="e2e test project")
            server.connection.project = project
            state["project"] = project
        # A checked-in function referencing a table by "<collection>/<table>" needs
        # the collection named at authoring time; the project around it is still
        # per-test, so a fixed name collides with nothing.
        name = name or f"e2e_{group}_{uuid.uuid4().hex[:8]}"
        document, secrets = _connection_document(conn)
        collection = Collection(
            connection=server.connection,
            name=name,
            group=group,
            connection_document=document,
            secrets=secrets,
        )
        collection.create()
        created.append(name)
        return collection

    yield make

    for name in reversed(created):
        server.delete_collection(name, raise_for_status=False)
    if state["project"]:
        server.delete_project(state["project"], raise_for_status=False)


def _connection_document(conn: Conn) -> tuple[str, list[dict]]:
    """Serialize ``conn`` to a dehydrated connection document (YAML) plus the
    ``[{key, value}]`` secret payload the apiserver injects into the function pod
    as ``SECRET_CONN__*`` env vars.

    ``Secret.to_dict()`` always strips the value, so each secret value is revealed
    from the hydrated ``Secret`` on ``conn`` -- located by the same leaf paths
    ``dehydrate`` harvests -- and re-injected before dehydrating (mirroring how the
    CLI harvests values from a raw connection YAML). Connections without secrets
    (e.g. LocalFile) yield an empty list, so this is a no-op for them."""
    from copy import deepcopy

    from tabsdatak.cli import template as _template
    from tabsdatak.common._json_serde import _secret_refs, dehydrate, replace_at

    reference = conn.to_dict()
    data = deepcopy(reference)
    for path, _ in _secret_refs(reference):
        secret = conn
        for attr in path.split("."):
            if attr not in ("$", "spec"):
                secret = getattr(secret, attr)
        replace_at(data, path, f"secret:{secret.value()}")
    dehydrated, secrets = dehydrate(data, reference)
    return _template.to_yaml(dehydrated), [
        {"key": kv.key, "value": kv.value} for kv in secrets
    ]
