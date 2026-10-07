#
# Copyright 2026 Tabsdata Inc.
#

"""The functions of a monkey run, one module-level function per entry of the
``fleet.json`` written next to this module for each run. Publishers sleep for
their configured seconds, cron publishers tick every minute, transformers and
subscribers chain from an upstream table, and stream publishers idle on their
run's own, empty topic. Transformers marked as failing raise after a few
seconds, so their runs fail and their executions stall."""

import json
import time
from pathlib import Path

import polars as pl

from tabsdatak.api import (
    CronTrigger,
    TableFrameSpec,
    TableFramesSpec,
    publisher,
    stream_publisher,
    subscriber,
    transformer,
)
from tabsdatak.conn.dev import Data, DataGenSrc, TrashDest
from tabsdatak.conn.kafka import KafkaStreamSrc

FLEET = json.loads((Path(__file__).parent / "fleet.json").read_text())

SOURCE = DataGenSrc(data=[Data(pl.DataFrame({"tick": [1]}))])

FAILING_SECONDS = 5

SCHEMA = json.dumps({
    "title": "Row",
    "type": "object",
    "properties": {"id": {"type": "integer"}},
    "required": ["id"],
})


def named(name: str, function):
    function.__name__ = name
    function.__qualname__ = name
    return function


def build_publisher(spec: dict):
    seconds = spec["seconds"]

    def body(frame: TableFrameSpec) -> TableFrameSpec:
        time.sleep(seconds)
        return frame

    decorate = publisher(source=SOURCE, output_tables=[spec["table"]])
    return decorate(named(spec["name"], body))


def build_cron_publisher(spec: dict):
    def body(frame: TableFrameSpec) -> TableFrameSpec:
        return frame

    decorate = publisher(
        source=SOURCE,
        output_tables=[spec["table"]],
        trigger_by=CronTrigger(mask="*/1 * * * *"),
    )
    return decorate(named(spec["name"], body))


def build_transformer(spec: dict):
    fails = spec["fails"]

    def body(frame: TableFrameSpec) -> TableFrameSpec:
        if fails:
            time.sleep(FAILING_SECONDS)
            raise RuntimeError("failing on purpose")
        return frame

    decorate = transformer(
        input_tables=[spec["upstream"]], output_tables=[spec["table"]]
    )
    return decorate(named(spec["name"], body))


def build_subscriber(spec: dict):
    def body(frame: TableFrameSpec) -> TableFrameSpec:
        return frame

    decorate = subscriber(destination=TrashDest(), input_tables=[spec["upstream"]])
    return decorate(named(spec["name"], body))


def build_stream_publisher(spec: dict):
    def body(inputs: TableFramesSpec) -> TableFrameSpec:
        frames = [frame for frame in (inputs or []) if frame is not None]
        return frames[0] if frames else None

    source = KafkaStreamSrc(topic=spec["topic"], data_format="json", schema=SCHEMA)
    decorate = stream_publisher(
        source=source, batch_max_secs=60, output_tables=[spec["table"]]
    )
    return decorate(named(spec["name"], body))


BUILDERS = {
    "publisher": build_publisher,
    "cron_publisher": build_cron_publisher,
    "stream_publisher": build_stream_publisher,
    "transformer": build_transformer,
    "subscriber": build_subscriber,
}

for entry in FLEET["functions"]:
    globals()[entry["name"]] = BUILDERS[entry["kind"]](entry)
