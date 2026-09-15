#
# Copyright 2026 Tabsdata Inc.
#

"""GCS source-plugin integration tests using tabsdata"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.gcs import GCSDomain, gcs_domain
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    assert_frames_match,
    run_function,
    table_frame,
    wait_for_completion,
)
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance
from testi_tabsdatak.conn.common.function_project import function_module
from testi_tabsdatak.conn.gcs.backend import run_prefix, src_conn, store

from tabsdatak.sdk.status_utils.execution import EXECUTION_SUCCESSFUL_FINAL_STATUSES
from tabsdatak.sdk.tabsdata_server import FunctionInfo

pytestmark = pytest.mark.gcs

# The functions these tests register live in `functions/`, checked in as a user
# project would have them; that directory is the project_root they are bundled
# from.
FUNCTIONS = Path(__file__).parent / "functions"
publisher = function_module(FUNCTIONS, "publisher")


def _entity_key(domain: GCSDomain, fmt: str) -> str:
    """The provisioned object a publisher's glob matches. The descriptor lists
    entities as ``<format>/<entity>_<stamp>.<format>``, relative to base_path."""
    prefix = f"{fmt}/{publisher.ENTITY}_"
    matches = sorted(key for key in (domain.entities or {}) if key.startswith(prefix))
    assert matches, f"no {prefix!r} entity in {sorted(domain.entities or {})}"
    return f"{str(domain.base_path).strip('/')}/{matches[0]}"


def _reference(domain: GCSDomain, source_format: str, tmp_path: Path) -> pl.DataFrame:
    """What the published table must hold. The staged model is identical across
    formats, so the csv copy is the reference for every one of them -- except that
    the transporter reads a quoted empty csv cell as null, where polars' own read
    keeps it as an empty string."""
    local = tmp_path / "reference.csv"
    store(domain).download(_entity_key(domain, "csv"), local)
    frame = pl.read_csv(local)
    if source_format == "csv":
        frame = frame.with_columns(pl.col(pl.String).replace("", None))
    return frame


@pytest.mark.parametrize(
    "source_format, function",
    [
        ("csv", publisher.pub_publisher_reads_csv_from_gcs_domain),
        ("ndjson", publisher.pub_publisher_reads_ndjson_from_gcs_domain),
        ("parquet", publisher.pub_publisher_reads_parquet_from_gcs_domain),
    ],
)
@tabsdata_instance(consumer="tdtest")
@gcs_domain("gcs_world_xs")
def test_publisher_reads_format_from_gcs_domain(
    source_format: str,
    function: FunctionInfo,
    instance_tdtest: Instance,
    domain_gcs_world_xs: GCSDomain,
    collection_factory: CollectionFactory,
    tmp_path: Path,
) -> None:
    domain = domain_gcs_world_xs
    expected = _reference(domain, source_format, tmp_path)
    collection = collection_factory(
        "sources", src_conn(domain, f"{domain.base_path}/{source_format}")
    )
    run_function(collection, function, FUNCTIONS)

    assert_frames_match(table_frame(collection, publisher.PUBLISHED_TABLE), expected)


# ---------------------------------------------------------------------------
# purge
#
# Purge deletes the objects the incremental cursor has moved past, so it only
# has anything to do on a resumed run. Object last-modified is set by the
# upload, so the two runs are ordered by staging between them rather than by
# stamping mtimes.
# ---------------------------------------------------------------------------


def _stage_parquet(backend, key: str, name: str, tmp_path: Path) -> None:
    local = tmp_path / name
    pl.DataFrame([{"name": name}]).write_parquet(local, use_pyarrow=True)
    store(backend).upload(local, key)


@tabsdata_instance(consumer="tdtest")
@gcs_domain("gcs")
def test_publisher_purges_the_objects_it_has_consumed(
    instance_tdtest: Instance,
    domain_gcs: GCSDomain,
    collection_factory: CollectionFactory,
    tmp_path: Path,
) -> None:
    domain = domain_gcs
    # A scratch prefix of this run's own: purge deletes, and the provisioned
    # world entities are shared with every other test.
    prefix = run_prefix(domain.base_path, "td-purge")
    first_key = f"{prefix}/first.parquet"
    second_key = f"{prefix}/second.parquet"
    _stage_parquet(domain, first_key, "first.parquet", tmp_path)

    collection = collection_factory("sources", src_conn(domain, prefix))
    function = collection.register_function(
        info=publisher.pub_publisher_purges_what_it_has_consumed,
        project_root=str(FUNCTIONS),
    )

    status = wait_for_completion(function.trigger())
    assert status in EXECUTION_SUCCESSFUL_FINAL_STATUSES
    assert_frames_match(
        table_frame(collection, publisher.PUBLISHED_TABLE),
        pl.DataFrame({"name": ["first.parquet"]}),
    )
    assert store(domain).exists(first_key), "nothing is behind the cursor yet"

    _stage_parquet(domain, second_key, "second.parquet", tmp_path)
    status = wait_for_completion(function.trigger())
    assert status in EXECUTION_SUCCESSFUL_FINAL_STATUSES
    assert_frames_match(
        table_frame(collection, publisher.PUBLISHED_TABLE),
        pl.DataFrame({"name": ["second.parquet"]}),
    )
    assert not store(domain).exists(first_key), "the consumed object was not purged"
    assert store(domain).exists(second_key), (
        "the object this run read is not purged until the next"
    )
