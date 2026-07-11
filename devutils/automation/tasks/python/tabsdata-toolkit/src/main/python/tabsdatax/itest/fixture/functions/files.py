#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end helpers shared by the file-based connectors (localfile, s3, gcs,
azureblob): discover a provisioned file/object, fabricate plugin contexts,
resolve the provisioned entity, and generate publisher / subscriber modules
parameterized by the connector's ``Src`` / ``Dest``.

Connector-agnostic: the caller passes the import line and the constructor
expression for its own source/destination, and (for object stores) an
:class:`ObjectStore` adapter over its own cloud client -- so nothing here
depends on a specific connector. Generic (non-file) harness lives in
:mod:`.harness`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

import pytest
from tabsdatak.sdk.tabsdata_server import FunctionInfo
from tabsdatak.spi import DestContext, SrcPluginCtx

from tabsdatax.itest.fixture.functions.harness import write_module


class _PluginCtx:
    """The fixed attributes every fabricated plugin context carries -- shared by
    the ``Src`` and ``Dest`` context stubs the integration tests drive the plugin
    with. The values are inert; the plugins only read ``work_dir`` in practice."""

    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "e2e-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "e2e-integ-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"

    @property
    def collection_id(self) -> str:
        return "test-collection-id"

    @property
    def collection_name(self) -> str:
        return "test-collection"

    @property
    def function_id(self) -> str:
        return "test-function-id"

    @property
    def function_name(self) -> str:
        return "test-function"

    def get_attr(self, attr):
        return None

    def set_attr(self, attr, val):
        pass

    def add_metadata(self, attr, val):
        pass


class PluginSrcCtx(_PluginCtx, SrcPluginCtx):
    """Fabricated ``SrcPluginCtx`` for driving a connector's ``SrcPlugin`` directly
    in an integration test."""


class PluginDestCtx(_PluginCtx, DestContext):
    """Fabricated ``DestContext`` for driving a connector's ``DestPlugin`` directly
    in an integration test."""


class ObjectStore(Protocol):
    """A connector-provided adapter over its cloud client (boto3 /
    google-cloud-storage / azure-storage-blob), so the shared object-store helpers
    stay client-agnostic. Keys are container/bucket-relative object paths."""

    def exists(self, key: str) -> bool: ...

    def download(self, key: str, dest: Path) -> None: ...

    def upload(self, local: Path, key: str) -> None: ...


def download_or_skip(store: ObjectStore, key: str, dest: Path) -> Path:
    """Download ``key`` to ``dest``, or skip if the object is absent -- the
    object-store analogue of :func:`first_provisioned_file`, so an unseeded or
    stale-descriptor domain yields a clean skip instead of a raw 404."""
    if not store.exists(key):
        pytest.skip(
            f"object {key!r} not provisioned; (re)seed the domain before running "
            f"this test",
        )
    store.download(key, dest)
    return dest


def entity_stem(domain) -> str:
    """A deterministic provisioned entity name from a cloud ``domain``. The
    descriptor lists entities as full object paths (``<prefix>/<fmt>/<entity>.<ext>``),
    one per staged format, so the bare name is the file stem; skip if none."""
    if not domain.entities:
        pytest.skip(
            f"domain {domain.name!r} has no entities (model={domain.model!r}); "
            f"seed the domain before running this test",
        )
    return sorted({Path(key).stem for key in domain.entities})[0]


def first_provisioned_file(base_dir: Path, pattern: str = "*.csv") -> Path:
    """The first file matching ``pattern`` in ``base_dir`` (resolved absolute), or
    skip. Absolute matters: the connection base_path is handed to the function
    pod, which resolves it against its own cwd -- a relative path would match no
    file there and the function would see empty input."""
    resolved = base_dir.resolve()
    matches = sorted(resolved.glob(pattern))
    if not matches:
        pytest.skip(
            f"no {pattern!r} file provisioned at {resolved}; seed the domain "
            f"before running this test",
        )
    return matches[0]


def write_publisher_module(
    project_root: Path,
    *,
    source_import: str,
    source_expr: str,
    output_table: str,
) -> FunctionInfo:
    """A publisher reading ``source_expr`` into ``output_table``. ``source_import``
    is the import line for the connector's ``Src`` (e.g.
    ``"from tabsdatak.conn.localfile import LocalFileSrc"``); ``source_expr`` is
    its constructor call. File ``Src``s are array-mode (a path may glob to many
    files), so the input is a list of frames; publish the single matched file."""
    module = write_module(
        project_root,
        "e2e_publisher",
        "from tabsdatak.api import publisher, TableFrameSpec, TableFramesSpec\n"
        f"{source_import}\n"
        "\n"
        f"@publisher(source={source_expr}, output_tables=[{output_table!r}])\n"
        "def e2e_pub(inputs: TableFramesSpec) -> TableFrameSpec:\n"
        "    frames = [f for f in (inputs or []) if f is not None]\n"
        "    return frames[0] if frames else None\n",
    )
    return module.e2e_pub


def write_subscriber_module(
    project_root: Path,
    *,
    dest_import: str,
    dest_expr: str,
    input_table: str,
) -> FunctionInfo:
    """A subscriber shipping ``input_table`` (a ``"collection/table"`` reference)
    to ``dest_expr``. ``dest_import`` is the import line for the connector's
    ``Dest``; ``dest_expr`` is its constructor call. A subscriber gets one frame
    per input table and returns the frames to write."""
    module = write_module(
        project_root,
        "e2e_subscriber",
        "from tabsdatak.api import subscriber, TableFrameSpec\n"
        f"{dest_import}\n"
        "\n"
        f"@subscriber(destination={dest_expr}, input_tables=[{input_table!r}])\n"
        "def e2e_sub(t: TableFrameSpec) -> TableFrameSpec:\n"
        "    return t\n",
    )
    return module.e2e_sub
