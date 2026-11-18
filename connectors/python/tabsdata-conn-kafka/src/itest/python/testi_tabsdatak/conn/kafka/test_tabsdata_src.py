#
# Copyright 2026 Tabsdata Inc.
#

"""Kafka stream-source integration tests using tabsdata"""

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.redpanda import RedpandaDomain, redpanda_domain
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    assert_frames_match,
)
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance
from testi_tabsdatak.conn.common.function_project import function_module
from testi_tabsdatak.conn.kafka.backend import broker, src_conn

from tabsdatak.sdk.tabsdata_server import Collection

pytestmark = pytest.mark.kafka

# The functions these tests register live in `functions/`, checked in as a user
# project would have them; that directory is the project_root they are bundled
# from.
FUNCTIONS = Path(__file__).parent / "functions"
publisher = function_module(FUNCTIONS, "publisher")

POLL_INTERVAL_SECONDS = 3
POLL_TIMEOUT_SECONDS = 120


def _published_rows(collection: Collection, table: str) -> pl.DataFrame | None:
    """Every row the committed data versions of `table` hold, across all of them:
    a stream stages its rows in as many batches as its triggers decide, so HEAD
    alone holds only the last one. A version that holds no data downloads nothing
    and is skipped."""
    handle = collection.get_table(table)
    if handle is None:
        return None
    frames = []
    with tempfile.TemporaryDirectory() as tmp:
        for index, version in enumerate(handle.dataversions):
            path = Path(tmp) / f"{index}.parquet"
            if handle.download(str(path), version=version) is not None:
                frames.append(pl.read_parquet(path))
    if not frames:
        return None
    return pl.concat(frames, how="diagonal_relaxed")


def _await_rows(collection: Collection, table: str, rows: int) -> pl.DataFrame:
    """A streamer stages a batch whenever a trigger is due while it keeps running, so
    the published rows are polled until they all land rather than read once."""
    deadline = time.time() + POLL_TIMEOUT_SECONDS
    while time.time() < deadline:
        published = _published_rows(collection, table)
        if published is not None and published.height >= rows:
            return published
        time.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError(
        f"table {table!r} did not reach {rows} rows within {POLL_TIMEOUT_SECONDS}s"
    )


@tabsdata_instance(consumer="tdtest")
@redpanda_domain("redpanda")
def test_stream_publisher_reads_topic_from_redpanda_domain(
    instance_tdtest: Instance,
    domain_redpanda: RedpandaDomain,
    collection_factory: CollectionFactory,
) -> None:
    domain = domain_redpanda
    bus = broker(domain)
    expected = pl.DataFrame(publisher.ROWS)
    collection = collection_factory("sources", src_conn(domain))
    with bus.fresh_topic(publisher.TOPIC) as topic:
        function = collection.register_function(
            info=publisher.pub_stream_publisher_reads_topic_from_redpanda_domain,
            project_root=str(FUNCTIONS),
        )
        try:
            bus.produce(topic, publisher.ROWS)
            published = _await_rows(
                collection, publisher.PUBLISHED_TABLE, expected.height
            )
            assert_frames_match(published.select(expected.columns), expected)
        finally:
            # The streamer runs until disabled; leaving it up leaks a pod past
            # teardown.
            function.enable(False)


@tabsdata_instance(consumer="tdtest")
@redpanda_domain("redpanda")
def test_stream_publisher_receives_ctx_from_redpanda_domain(
    instance_tdtest: Instance,
    domain_redpanda: RedpandaDomain,
    collection_factory: CollectionFactory,
) -> None:
    domain = domain_redpanda
    bus = broker(domain)
    rows = len(publisher.ROWS)
    collection = collection_factory("sources", src_conn(domain))
    with bus.fresh_topic(publisher.CTX_TOPIC) as topic:
        function = collection.register_function(
            info=publisher.pub_stream_publisher_receives_ctx_from_redpanda_domain,
            project_root=str(FUNCTIONS),
        )
        try:
            bus.produce(topic, publisher.ROWS)
            _await_rows(collection, publisher.CTX_PUBLISHED_TABLE, rows)
            bus.produce(topic, publisher.ROWS)
            published = _await_rows(collection, publisher.CTX_PUBLISHED_TABLE, 2 * rows)
        finally:
            function.enable(False)

    # Each batch is one transaction, and the state one batch sets is the state
    # the next one reads: the counter and the transaction id advance together.
    # A run that publishes nothing, such as the one the trigger starts, counts
    # too, so the first published counter is not necessarily 1.
    batches = published.select(
        publisher.CTX_BATCH_COLUMN, publisher.CTX_TRANSACTION_COLUMN
    ).unique()
    counters = sorted(batches[publisher.CTX_BATCH_COLUMN].to_list())
    transactions = batches[publisher.CTX_TRANSACTION_COLUMN].to_list()
    assert len(counters) >= 2
    assert counters[0] >= 1
    assert len(set(counters)) == len(counters)
    assert len(set(transactions)) == len(counters)
