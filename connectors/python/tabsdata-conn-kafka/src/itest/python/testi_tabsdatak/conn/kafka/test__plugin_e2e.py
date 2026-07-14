#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end Kafka connector tests: register a real ``@stream_publisher`` on a
running tabsdata instance, let the apiserver dispatch the streamer pod, and assert
the messages it consumes from a redpanda topic land in the published table --
unlike ``test__plugin_integration.py``, which drives ``KafkaStreamSrcPlugin.run``
directly with a fabricated context.

Kafka is a streaming source: the streamer runs continuously, staging a batch per
rollover. The test seeds a topic, triggers the function, polls the output table
until the batch lands (``kafka.messages_rollover`` stages within seconds, so no
1-minute flush wait), verifies, then ``enable(False)`` stops the stream so
teardown doesn't leak a running pod. Requires the ``tdtest`` instance up and the
``redpanda`` backend provisioned.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.redpanda import RedpandaDomain, redpanda_domain
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    assert_frames_match,
    write_module,
)
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance

from tabsdatak.conn.kafka import KafkaStreamSrcConn
from tabsdatak.sdk.tabsdata_server import FunctionInfo

pytestmark = pytest.mark.kafka

_TABLE = "e2e_kafka_tbl"
_ROWS = [{"id": i, "name": f"n{i}"} for i in range(1, 6)]
# KafkaStreamSrc requires a schema even for JSON (self-describing on the wire, but
# the connector uses it to type the columns).
_SCHEMA = json.dumps({
    "title": "Row",
    "type": "object",
    "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
    "required": ["id", "name"],
})


def _write_stream_publisher(
    project_root: Path, *, topic: str, output_table: str, messages_rollover: int
) -> FunctionInfo:
    """A ``@stream_publisher`` consuming ``topic`` (JSON, no schema registry) into
    ``output_table``, rolling over every ``messages_rollover`` messages so a batch
    stages within seconds."""
    src_expr = (
        f"KafkaStreamSrc(topic={topic!r}, data_format='json', schema={_SCHEMA!r}, "
        f"src_cfg={{'kafka.messages_rollover': {messages_rollover}, "
        f"'kafka.consume_timeout_seconds': 2}})"
    )
    module = write_module(
        project_root,
        "e2e_stream_publisher",
        "from tabsdatak.api import stream_publisher, TableFrameSpec, TableFramesSpec\n"
        "from tabsdatak.conn.kafka import KafkaStreamSrc\n"
        "\n"
        f"@stream_publisher(source={src_expr}, flush_interval_mins=1, "
        f"output_tables=[{output_table!r}])\n"
        "def e2e_stream_pub(inputs: TableFramesSpec) -> tuple[TableFrameSpec]:\n"
        "    frames = [f for f in (inputs or []) if f is not None]\n"
        "    return (frames[0] if frames else None,)\n",
    )
    return module.e2e_stream_pub


def _poll_table(collection, table: str, *, expected_rows: int, timeout: float):
    deadline = time.time() + timeout
    while time.time() < deadline:
        published = collection.get_table(table)
        if published is not None:
            df = published.sample()
            if df is not None and df.height >= expected_rows:
                return df
        time.sleep(3)
    raise AssertionError(
        f"table {table!r} did not reach {expected_rows} rows within {timeout}s"
    )


@tabsdata_instance(consumer="tdtest")
@redpanda_domain("redpanda")
def test_stream_publisher_reads_topic_from_redpanda_domain(
    instance_tdtest: Instance,
    domain_redpanda: RedpandaDomain,
    collection_factory: CollectionFactory,
    kafka,
    tmp_path: Path,
) -> None:
    k = kafka(domain_redpanda)
    topic = k.fresh_topic()
    project_root = tmp_path / "pub"
    project_root.mkdir()
    info = _write_stream_publisher(
        project_root,
        topic=topic,
        output_table=_TABLE,
        messages_rollover=len(_ROWS),
    )
    collection = collection_factory(
        "sources",
        KafkaStreamSrcConn(servers=k.servers, group_id=f"g_{uuid.uuid4().hex[:8]}"),
    )
    function = collection.register_function(info=info, project_root=str(project_root))
    try:
        k.produce_json(topic, _ROWS)
        function.trigger()
        got = _poll_table(collection, _TABLE, expected_rows=len(_ROWS), timeout=120)
        assert_frames_match(got.select(["id", "name"]), pl.DataFrame(_ROWS))
    finally:
        try:
            function.enable(False)
        except Exception:
            pass
        k.delete_topic(topic)
