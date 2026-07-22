#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end LocalFile connector tests: register a real function on a running
tabsdata instance, trigger it, and assert the outcome against a tdx-provisioned
``fs`` domain -- unlike ``test__plugin_integration.py``, which drives the plugin
directly with a fabricated context.

The generic harness (``server`` / ``collection_factory`` fixtures, ``run_function``,
``assert_frames_match``) and the file-connector helpers live under
``tabsdatax.itest.fixture.functions`` (``harness`` / ``files``); only the LocalFile
specifics are here. Requires the ``tdtest`` instance up and the ``fs`` domain seeded
with data (the seed lays the model out per format under
``base_path/{csv,json,parquet}/``).
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.functions.files import (
    first_provisioned_file,
    write_publisher_module,
    write_subscriber_module,
)
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    assert_frames_match,
    run_function,
)
from tabsdatax.itest.fixture.instances.tabsdata import (
    Instance,
    tabsdata_instance,
)

from tabsdatak.conn.localfile import LocalFileDestConn, LocalFileSrcConn

pytestmark = pytest.mark.localfile

_SRC_IMPORT = "from tabsdatak.conn.localfile import LocalFileSrc"
_DEST_IMPORT = "from tabsdatak.conn.localfile import LocalFileDest"
_TABLE = "e2e_localfile_tbl"
# The seed writes each format into its own subdir (subdir name == extension); it
# stages JSON as NDJSON (``.ndjson``), the only JSON shape LocalFile reads.
_SOURCE_FORMATS = ("csv", "ndjson", "parquet")


def _source_file(domain_fs: FsDomain, fmt: str) -> Path:
    """The provisioned source file for ``fmt`` (data lives in per-format subdirs)."""
    return first_provisioned_file(domain_fs.base_path / fmt, f"*.{fmt}")


def _publish(
    collection_factory: CollectionFactory, project_root: Path, source_file: Path
) -> object:
    """Publish ``source_file`` into a fresh source collection; return the
    collection (its LocalFile connection is rooted at the file's directory)."""
    info = write_publisher_module(
        project_root,
        source_import=_SRC_IMPORT,
        source_expr=f"LocalFileSrc(paths=[{source_file.name!r}])",
        output_table=_TABLE,
    )
    collection = collection_factory(
        "sources", LocalFileSrcConn(base_path=str(source_file.parent))
    )
    run_function(collection, info, project_root)
    return collection


@pytest.fixture
def expected(domain_fs: FsDomain) -> pl.DataFrame:
    """Canonical reference data. The world model is identical across formats, so
    the csv copy is ground truth for every source/destination format."""
    return pl.read_csv(_source_file(domain_fs, "csv"))


@pytest.mark.parametrize("source_format", _SOURCE_FORMATS)
@tabsdata_instance(consumer="tdtest")
@fs_domain("fs")
def test_publisher_reads_format_from_fs_domain(
    source_format: str,
    instance_tdtest: Instance,
    domain_fs: FsDomain,
    expected: pl.DataFrame,
    collection_factory: CollectionFactory,
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "pub"
    project_root.mkdir()
    collection = _publish(
        collection_factory, project_root, _source_file(domain_fs, source_format)
    )

    published = collection.get_table(_TABLE)
    assert published is not None, f"table {_TABLE!r} not found after publish"
    assert_frames_match(published.sample(), expected)


@pytest.mark.parametrize(
    "dest_format, reader",
    [
        ("csv", pl.read_csv),
        ("json", pl.read_ndjson),
        ("parquet", pl.read_parquet),
    ],
)
@tabsdata_instance(consumer="tdtest")
@fs_domain("fs")
def test_subscriber_writes_format_to_fs_domain(
    dest_format: str,
    reader: Callable[[Path], pl.DataFrame],
    instance_tdtest: Instance,
    domain_fs: FsDomain,
    expected: pl.DataFrame,
    collection_factory: CollectionFactory,
    tmp_path: Path,
) -> None:
    # Round-trip: publish the provisioned CSV into a table, then subscribe it back
    # out to a file in `dest_format` and assert the file matches. OS/kminus does
    # not auto-trigger subscribers, so the subscriber is triggered directly.
    publisher_root = tmp_path / "pub"
    publisher_root.mkdir()
    source_collection = _publish(
        collection_factory, publisher_root, _source_file(domain_fs, "csv")
    )

    out_dir = tmp_path / "out"
    out_dir.mkdir()
    out_name = f"e2e_result.{dest_format}"
    subscriber_root = tmp_path / "sub"
    subscriber_root.mkdir()
    subscriber_info = write_subscriber_module(
        subscriber_root,
        dest_import=_DEST_IMPORT,
        dest_expr=f"LocalFileDest(paths=[{out_name!r}])",
        input_table=f"{source_collection.name}/{_TABLE}",
    )
    destination_collection = collection_factory(
        "destinations", LocalFileDestConn(base_path=str(out_dir))
    )
    run_function(destination_collection, subscriber_info, subscriber_root)

    out_file = out_dir / out_name
    assert out_file.exists(), f"subscriber did not write {out_file}"
    assert_frames_match(reader(out_file), expected)
