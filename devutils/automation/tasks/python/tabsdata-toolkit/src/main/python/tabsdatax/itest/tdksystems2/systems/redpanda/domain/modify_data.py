#
# Copyright 2026 Tabsdata Inc.
#

"""Push another batch of the model's data to a redpanda domain's topics.

Kafka is append-only, so modify-data does not apply the shared delta recipe
(insert/update/delete on specific rows); instead it produces a fresh batch of the
model's rows as new JSON messages to every ``<domain>.<entity>`` topic. ``--size``
sets the batch multiplier (as in create-data); successive runs advance the seed so
models with random data vary (fixed reference models re-emit the same rows).
"""

from __future__ import annotations

import argparse
import json
import random

from tabsdatax.itest.tdksystems2 import cli, console, constants, datagen, descriptor
from tabsdatax.itest.tdksystems2.errors import TdxError


def add_arguments(parser: argparse.ArgumentParser) -> None:
    cli.add_domain(parser)
    cli.add_domain_dir(parser)
    cli.add_size(parser)
    cli.add_out(parser)
    cli.add_latch(parser)


def run(ctx: cli.Context) -> int:
    assert isinstance(ctx.info, descriptor.DomainInfo) and ctx.path is not None
    a = ctx.args
    info = ctx.info
    sysobj = ctx.system
    if not info.model:
        raise TdxError(f"domain '{ctx.name}' has no model — run create-model first")
    if a.size < 1:
        raise TdxError(f"{constants.OPT_SIZE} must be >= 1 (1 = base)")
    seed = info.seed + 1  # advance so successive batches vary (fixed models re-emit)
    n = datagen.BASE_SIZE * a.size
    tables = datagen.load_model(info.model).build_initial(
        random.Random(datagen.INITIAL_SEED + seed), n
    )
    uri = sysobj.data_plane_uri(info.meta)
    produced = sysobj.load_tables(uri, tables)
    counts = dict(info.entities)
    for topic, count in produced.items():
        counts[topic] = counts.get(topic, 0) + count  # kafka log appends
    descriptor.record_changes(ctx.path, counts, seed=seed)
    if a.out == constants.OUT_JSON:
        report = {topic: datagen.iud(insert=n) for topic, n in produced.items()}
        console.result(json.dumps(report, indent=2))
    else:
        console.good(
            f"pushed {sum(produced.values())} message(s) to {len(produced)} "
            f"topic(s) in domain '{ctx.name}'"
        )
    return 0
