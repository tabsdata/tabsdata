#
# Copyright 2026 Tabs Data Inc.
#

"""End-to-end GCS connector tests: register a real function on a running tabsdata
instance, trigger it, and assert the outcome against a tdx-provisioned ``gcs``
domain -- unlike ``test__plugin_integration.py``, which drives the plugin
directly with a fabricated context.

The generic harness (``server`` / ``collection_factory`` fixtures, ``run_function``,
``assert_frames_match``) and the file-connector helpers live under
``tabsdatax.itest.fixture.functions`` (``harness`` / ``files``); only the GCS
specifics are here. Requires the ``tdtest`` instance up and the ``gcs`` domain
provisioned with data (``tdx-domain seed``); the seed stages each format under
``<prefix>/{csv,ndjson,parquet}/<entity>.<ext>`` in the bucket.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.gcs import GcsDomain, gcs_domain
from tabsdatax.itest.fixture.functions.files import (
    write_publisher_module,
    write_subscriber_module,
)
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    assert_frames_match,
    run_function,
)
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import GCPJson
from tabsdatak.conn.gcs import GCSDestConn, GCSSrcConn

pytestmark = pytest.mark.gcs

_SRC_IMPORT = "from tabsdatak.conn.gcs import GCSSrc"
_DEST_IMPORT = "from tabsdatak.conn.gcs import GCSDest"
_TABLE = "e2e_gcs_tbl"
# The seed stages each format under <prefix>/<fmt>/<entity>.<ext>; JSON is
# staged as NDJSON (``.ndjson``), the only JSON shape the connector reads.
_SOURCE_FORMATS = ("csv", "ndjson", "parquet")


def _credentials(domain: GcsDomain) -> GCPJson:
    return GCPJson(json=Secret(domain.service_account_key))


def _entity(domain: GcsDomain) -> str:
    """A deterministic provisioned entity name. The descriptor lists entities as
    full object paths (``<prefix>/<fmt>/<entity>.<ext>``), one per staged format,
    so the bare name is the file stem."""
    if not domain.entities:
        pytest.skip(
            f"gcs domain {domain.name!r} has no entities (model={domain.model!r}); "
            "seed the domain before running this test",
        )
    return sorted({Path(key).stem for key in domain.entities})[0]


@pytest.fixture
def gcs_client(domain_gcs: GcsDomain):
    from google.cloud import storage

    return storage.Client.from_service_account_info(
        json.loads(domain_gcs.service_account_key)
    )


def _download(gcs_client, bucket: str, key: str, dest: Path) -> Path:
    gcs_client.bucket(bucket).blob(key).download_to_filename(str(dest))
    return dest


@pytest.fixture
def expected(domain_gcs: GcsDomain, gcs_client, tmp_path: Path) -> pl.DataFrame:
    """Canonical reference data: the provisioned csv copy of the chosen entity,
    downloaded from the bucket. The model is identical across formats, so it is
    ground truth for every source/destination format."""
    entity = _entity(domain_gcs)
    key = f"{domain_gcs.base_path}/csv/{entity}.csv"
    return pl.read_csv(
        _download(gcs_client, domain_gcs.bucket, key, tmp_path / "ref.csv")
    )


def _publish(
    collection_factory: CollectionFactory,
    domain_gcs: GcsDomain,
    project_root: Path,
    source_path: str,
) -> object:
    """Publish ``source_path`` (relative to the domain prefix) into a fresh
    source collection; return the collection."""
    info = write_publisher_module(
        project_root,
        source_import=_SRC_IMPORT,
        source_expr=f"GCSSrc(paths=[{source_path!r}])",
        output_table=_TABLE,
    )
    collection = collection_factory(
        "sources",
        GCSSrcConn(
            bucket=domain_gcs.bucket,
            credentials=_credentials(domain_gcs),
            base_path=str(domain_gcs.base_path),
        ),
    )
    run_function(collection, info, project_root)
    return collection


@pytest.mark.parametrize("source_format", _SOURCE_FORMATS)
@tabsdata_instance(consumer="tdtest")
@gcs_domain("gcs")
def test_publisher_reads_format_from_gcs_domain(
    source_format: str,
    instance_tdtest: Instance,
    domain_gcs: GcsDomain,
    expected: pl.DataFrame,
    collection_factory: CollectionFactory,
    tmp_path: Path,
) -> None:
    entity = _entity(domain_gcs)
    project_root = tmp_path / "pub"
    project_root.mkdir()
    collection = _publish(
        collection_factory,
        domain_gcs,
        project_root,
        f"{source_format}/{entity}.{source_format}",
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
@gcs_domain("gcs")
def test_subscriber_writes_format_to_gcs_domain(
    dest_format: str,
    reader: Callable[[Path], pl.DataFrame],
    instance_tdtest: Instance,
    domain_gcs: GcsDomain,
    expected: pl.DataFrame,
    collection_factory: CollectionFactory,
    gcs_client,
    tmp_path: Path,
) -> None:
    # Round-trip: publish the provisioned csv into a table, then subscribe it
    # back out to an object in `dest_format` and assert the object matches.
    # OS/kminus does not auto-trigger subscribers, so trigger it directly.
    entity = _entity(domain_gcs)
    publisher_root = tmp_path / "pub"
    publisher_root.mkdir()
    source_collection = _publish(
        collection_factory, domain_gcs, publisher_root, f"csv/{entity}.csv"
    )

    out_prefix = f"{domain_gcs.base_path}/e2e-out/{uuid.uuid4().hex}"
    out_name = f"e2e_result.{dest_format}"
    subscriber_root = tmp_path / "sub"
    subscriber_root.mkdir()
    subscriber_info = write_subscriber_module(
        subscriber_root,
        dest_import=_DEST_IMPORT,
        dest_expr=f"GCSDest(paths=[{out_name!r}])",
        input_table=f"{source_collection.name}/{_TABLE}",
    )
    destination_collection = collection_factory(
        "destinations",
        GCSDestConn(
            bucket=domain_gcs.bucket,
            credentials=_credentials(domain_gcs),
            base_path=out_prefix,
        ),
    )
    run_function(destination_collection, subscriber_info, subscriber_root)

    fetched = _download(
        gcs_client,
        domain_gcs.bucket,
        f"{out_prefix}/{out_name}",
        tmp_path / out_name,
    )
    assert_frames_match(reader(fetched), expected)
