#
# Copyright 2026 Tabs Data Inc.
#

"""End-to-end S3 connector tests: register a real function on a running tabsdata
instance, trigger it, and assert the outcome against a tdx-provisioned ``s3``
domain -- unlike ``test__plugin_integration.py``, which drives the plugin
directly with a fabricated context.

The generic harness (``server`` / ``collection_factory`` fixtures, ``run_function``,
``assert_frames_match``) and the file-connector helpers live under
``tabsdatax.itest.fixture.functions`` (``harness`` / ``files``); only the S3
specifics are here. Requires the ``tdtest`` instance up and the ``s3`` domain
provisioned with data (``tdx-domain seed``); the seed stages each format under
``<prefix>/{csv,ndjson,parquet}/<entity>.<ext>`` in the bucket.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.s3 import S3Domain, s3_domain
from tabsdatax.itest.fixture.functions.files import (
    ObjectStore,
    download_or_skip,
    entity_stem,
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
from tabsdatak.conn.common.types import AwsAccessSecretKey
from tabsdatak.conn.s3 import S3DestConn, S3SrcConn

pytestmark = pytest.mark.s3

_SRC_IMPORT = "from tabsdatak.conn.s3 import S3Src"
_DEST_IMPORT = "from tabsdatak.conn.s3 import S3Dest"
_TABLE = "e2e_s3_tbl"
# The seed stages each format under <prefix>/<fmt>/<entity>.<ext>; JSON is
# staged as NDJSON (``.ndjson``), the only JSON shape the connector reads.
_SOURCE_FORMATS = ("csv", "ndjson", "parquet")


def _credentials(domain: S3Domain) -> AwsAccessSecretKey:
    return AwsAccessSecretKey(
        access_key_id=Secret(domain.access_key),
        secret_access_key=Secret(domain.secret_key),
    )


@pytest.fixture
def store(domain_s3: S3Domain, object_store) -> ObjectStore:
    return object_store(domain_s3)


@pytest.fixture
def expected(domain_s3: S3Domain, store: ObjectStore, tmp_path: Path) -> pl.DataFrame:
    """Canonical reference data: the provisioned csv copy of the chosen entity,
    downloaded from the bucket. The model is identical across formats, so it is
    ground truth for every source/destination format."""
    key = f"{domain_s3.base_path}/csv/{entity_stem(domain_s3)}.csv"
    return pl.read_csv(download_or_skip(store, key, tmp_path / "ref.csv"))


def _publish(
    collection_factory: CollectionFactory,
    domain_s3: S3Domain,
    project_root: Path,
    source_path: str,
) -> object:
    """Publish ``source_path`` (relative to the domain prefix) into a fresh
    source collection; return the collection."""
    info = write_publisher_module(
        project_root,
        source_import=_SRC_IMPORT,
        source_expr=f"S3Src(paths=[{source_path!r}])",
        output_table=_TABLE,
    )
    collection = collection_factory(
        "sources",
        S3SrcConn(
            bucket=domain_s3.bucket,
            region=domain_s3.region,
            credentials=_credentials(domain_s3),
            base_path="/" + str(domain_s3.base_path).strip("/"),
        ),
    )
    run_function(collection, info, project_root)
    return collection


@pytest.mark.parametrize("source_format", _SOURCE_FORMATS)
@tabsdata_instance(consumer="tdtest")
@s3_domain("s3")
def test_publisher_reads_format_from_s3_domain(
    source_format: str,
    instance_tdtest: Instance,
    domain_s3: S3Domain,
    expected: pl.DataFrame,
    collection_factory: CollectionFactory,
    tmp_path: Path,
) -> None:
    entity = entity_stem(domain_s3)
    project_root = tmp_path / "pub"
    project_root.mkdir()
    collection = _publish(
        collection_factory,
        domain_s3,
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
@s3_domain("s3")
def test_subscriber_writes_format_to_s3_domain(
    dest_format: str,
    reader: Callable[[Path], pl.DataFrame],
    instance_tdtest: Instance,
    domain_s3: S3Domain,
    expected: pl.DataFrame,
    collection_factory: CollectionFactory,
    store: ObjectStore,
    tmp_path: Path,
) -> None:
    # Round-trip: publish the provisioned csv into a table, then subscribe it
    # back out to an object in `dest_format` and assert the object matches.
    # OS/kminus does not auto-trigger subscribers, so trigger it directly.
    entity = entity_stem(domain_s3)
    publisher_root = tmp_path / "pub"
    publisher_root.mkdir()
    source_collection = _publish(
        collection_factory, domain_s3, publisher_root, f"csv/{entity}.csv"
    )

    out_prefix = f"{domain_s3.base_path}/e2e-out/{uuid.uuid4().hex}"
    out_name = f"e2e_result.{dest_format}"
    subscriber_root = tmp_path / "sub"
    subscriber_root.mkdir()
    subscriber_info = write_subscriber_module(
        subscriber_root,
        dest_import=_DEST_IMPORT,
        dest_expr=f"S3Dest(paths=[{out_name!r}])",
        input_table=f"{source_collection.name}/{_TABLE}",
    )
    destination_collection = collection_factory(
        "destinations",
        S3DestConn(
            bucket=domain_s3.bucket,
            region=domain_s3.region,
            credentials=_credentials(domain_s3),
            base_path="/" + out_prefix.strip("/"),
        ),
    )
    run_function(destination_collection, subscriber_info, subscriber_root)

    fetched = download_or_skip(store, f"{out_prefix}/{out_name}", tmp_path / out_name)
    assert_frames_match(reader(fetched), expected)
