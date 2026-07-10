#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end helpers shared by the file-based connectors (localfile, s3, gcs,
azureblob): discover a provisioned file in a domain, and generate publisher /
subscriber modules parameterized by the connector's ``Src`` / ``Dest``.

Connector-agnostic: the caller passes the import line and the constructor
expression for its own source/destination, so nothing here depends on a
specific connector. Generic (non-file) harness lives in :mod:`.harness`.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatak.sdk.tabsdata_server import FunctionInfo

from tabsdatax.itest.fixture.functions.harness import write_module


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
