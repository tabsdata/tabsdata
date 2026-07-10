#
# Copyright 2026 Tabs Data Inc.
#

"""End-to-end LocalFile connector tests: register a real function on a running
tabsdata instance, trigger it, and assert the outcome against a tdx-provisioned
``fs`` domain -- unlike ``test__plugin_integration.py``, which drives the plugin
directly with a fabricated context.

Requires the ``tdtest`` instance (``@tabsdata_instance``) to be up and the ``fs``
system/domain to be provisioned with data (``tdx-domain seed``); both fixtures
skip/fail loudly otherwise.
"""

from __future__ import annotations

import importlib.util
import sys
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from time import sleep
from types import ModuleType

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.instances.tabsdata import (
    Instance,
    tabsdata_instance,
)

from tabsdatak import _template
from tabsdatak.conn.localfile import LocalFileDestConn, LocalFileSrcConn
from tabsdatak.sdk.status_utils.execution import (
    EXECUTION_FINAL_STATUSES,
    EXECUTION_SUCCESSFUL_FINAL_STATUSES,
)
from tabsdatak.sdk.tabsdata_server import Collection, FunctionInfo, TabsdataServer
from tabsdatak.spi import Conn

pytestmark = pytest.mark.localfile

_POLL_INTERVAL_SECONDS = 2
_POLL_TIMEOUT_SECONDS = 300

# Creates a collection in a given group ("sources" / "destinations") with the
# given LocalFile connection, and returns the created ``Collection``.
CollectionFactory = Callable[[str, Conn], Collection]


# --- module generators ----------------------------------------------------
#
# Function definitions must live in real modules under a ``project_root`` (they
# are bundled and re-imported by the runner), and the source/destination paths
# are fixed at decoration time -- so the modules are generated per test.


def _write_module(project_root: Path, module_name: str, body: str) -> ModuleType:
    (project_root / f"{module_name}.py").write_text(body)
    spec = importlib.util.spec_from_file_location(
        module_name, project_root / f"{module_name}.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _write_publisher_module(
    project_root: Path, csv_name: str, table: str
) -> FunctionInfo:
    """A publisher reading ``csv_name`` (relative to the collection's LocalFile
    connection base_path). ``LocalFileSrc`` is array-mode (a path may glob to many
    files), so the input is a list of frames; publish the single matched file."""
    module = _write_module(
        project_root,
        "e2e_publisher",
        "from tabsdatak.api import publisher, TableFrameSpec, TableFramesSpec\n"
        "from tabsdatak.conn.localfile import LocalFileSrc\n"
        "\n"
        f"@publisher(source=LocalFileSrc(paths=[{csv_name!r}]), "
        f"output_tables=[{table!r}])\n"
        "def e2e_pub(inputs: TableFramesSpec) -> TableFrameSpec:\n"
        "    frames = [f for f in (inputs or []) if f is not None]\n"
        "    return frames[0] if frames else None\n",
    )
    return module.e2e_pub


def _write_subscriber_module(
    project_root: Path, table_ref: str, out_name: str
) -> FunctionInfo:
    """A subscriber shipping ``table_ref`` (``"collection/table"``) to ``out_name``
    (relative to the collection's LocalFile connection base_path). A subscriber
    gets one frame per input table and returns the frames to write."""
    module = _write_module(
        project_root,
        "e2e_subscriber",
        "from tabsdatak.api import subscriber, TableFrameSpec\n"
        "from tabsdatak.conn.localfile import LocalFileDest\n"
        "\n"
        f"@subscriber(destination=LocalFileDest(paths=[{out_name!r}]), "
        f"input_tables=[{table_ref!r}])\n"
        "def e2e_sub(t: TableFrameSpec) -> TableFrameSpec:\n"
        "    return t\n",
    )
    return module.e2e_sub


# --- harness ---------------------------------------------------------------


def _wait_for_completion(execution) -> str:
    waited = 0
    while execution.status not in EXECUTION_FINAL_STATUSES:
        if waited >= _POLL_TIMEOUT_SECONDS:
            pytest.fail(
                f"execution did not finish within {_POLL_TIMEOUT_SECONDS}s "
                f"(last status {execution.status!r})",
                pytrace=False,
            )
        sleep(_POLL_INTERVAL_SECONDS)
        waited += _POLL_INTERVAL_SECONDS
        execution.refresh()
    return execution.status


def _run(
    collection: Collection, function_info: FunctionInfo, project_root: Path
) -> None:
    """Register ``function_info`` in ``collection``, trigger it, assert success."""
    function = collection.register_function(
        info=function_info, project_root=str(project_root)
    )
    status = _wait_for_completion(function.trigger())
    assert status in EXECUTION_SUCCESSFUL_FINAL_STATUSES, (
        f"execution finished with non-success status {status!r}"
    )


@pytest.fixture
def server(instance_tdtest: Instance) -> TabsdataServer:
    api = instance_tdtest.apiserver
    return TabsdataServer(
        f"{api.host}:{api.port}", api.superuser.user, api.superuser.password
    )


@pytest.fixture
def source_csv(domain_fs: FsDomain) -> Path:
    """The first CSV already provisioned in the fs domain, as an absolute path.

    Absolute matters: the connection base_path is handed to the function pod,
    which resolves it against its own cwd -- a relative domain path would match
    no file there and the publisher would emit an empty table.
    """
    base_dir = domain_fs.base_path.resolve()
    sources = sorted(base_dir.glob("*.csv"))
    if not sources:
        pytest.skip(
            f"fs domain {domain_fs.name!r} has no CSV at {base_dir} "
            f"(model={domain_fs.model!r}); provision data before running this test",
        )
    return sources[0]


@pytest.fixture
def collection_factory(server: TabsdataServer) -> Iterator[CollectionFactory]:
    """Create source/destination collections under a per-test project, cleaning
    up every collection and the project on teardown. The project is created
    lazily on first use so a skipped test provisions nothing."""
    state: dict[str, str | None] = {"project": None}
    created: list[str] = []

    def make(group: str, conn: Conn) -> Collection:
        if state["project"] is None:
            project = f"e2e_localfile_{uuid.uuid4().hex[:8]}_proj"
            server.create_project(project, description="localfile e2e test project")
            server.connection.project = project
            state["project"] = project
        name = f"e2e_localfile_{group}_{uuid.uuid4().hex[:8]}"
        collection = Collection(
            connection=server.connection,
            name=name,
            group=group,
            connection_document=_template.to_yaml(conn.to_dict()),
        )
        collection.create()
        created.append(name)
        return collection

    yield make

    for name in reversed(created):
        server.delete_collection(name, raise_for_status=False)
    if state["project"]:
        server.delete_project(state["project"], raise_for_status=False)


def _assert_matches(got: pl.DataFrame, expected: pl.DataFrame) -> None:
    """Order- and dtype-agnostic content equality on ``expected``'s columns
    (transporter format inference may differ from polars' own read)."""
    columns = expected.columns
    assert (
        got.select(columns).cast(pl.String).sort(by=columns).to_dicts()
        == expected.cast(pl.String).sort(by=columns).to_dicts()
    )


# --- tests -----------------------------------------------------------------


@tabsdata_instance(consumer="tdtest")
@fs_domain("fs")
@pytest.mark.aleix_dev
def test_publisher_reads_csv_from_fs_domain(
    source_csv: Path,
    collection_factory: CollectionFactory,
    tmp_path: Path,
) -> None:
    expected = pl.read_csv(source_csv)
    table = "e2e_localfile_tbl"

    project_root = tmp_path / "pub"
    project_root.mkdir()
    publisher_info = _write_publisher_module(project_root, source_csv.name, table)

    collection = collection_factory(
        "sources", LocalFileSrcConn(base_path=str(source_csv.parent))
    )
    _run(collection, publisher_info, project_root)

    published = collection.get_table(table)
    assert published is not None, f"table {table!r} not found after publish"
    _assert_matches(published.sample(), expected)


@pytest.mark.parametrize(
    "extension, reader",
    [
        ("csv", pl.read_csv),
        ("json", pl.read_ndjson),
        ("parquet", pl.read_parquet),
    ],
)
@tabsdata_instance(consumer="tdtest")
@fs_domain("fs")
@pytest.mark.aleix_dev
def test_subscriber_writes_table_in_format(
    extension: str,
    reader: Callable[[Path], pl.DataFrame],
    source_csv: Path,
    collection_factory: CollectionFactory,
    tmp_path: Path,
) -> None:
    # Round-trip: publish the provisioned CSV into a table, then subscribe it back
    # out to a file in `extension` format and assert the file matches. OS/kminus
    # does not auto-trigger subscribers, so the subscriber is triggered directly.
    expected = pl.read_csv(source_csv)
    table = "e2e_localfile_tbl"

    publisher_root = tmp_path / "pub"
    publisher_root.mkdir()
    publisher_info = _write_publisher_module(publisher_root, source_csv.name, table)
    source_collection = collection_factory(
        "sources", LocalFileSrcConn(base_path=str(source_csv.parent))
    )
    _run(source_collection, publisher_info, publisher_root)

    out_dir = tmp_path / "out"
    out_dir.mkdir()
    out_name = f"e2e_result.{extension}"
    subscriber_root = tmp_path / "sub"
    subscriber_root.mkdir()
    subscriber_info = _write_subscriber_module(
        subscriber_root, f"{source_collection.name}/{table}", out_name
    )
    destination_collection = collection_factory(
        "destinations", LocalFileDestConn(base_path=str(out_dir))
    )
    _run(destination_collection, subscriber_info, subscriber_root)

    out_file = out_dir / out_name
    assert out_file.exists(), f"subscriber did not write {out_file}"
    _assert_matches(reader(out_file), expected)
