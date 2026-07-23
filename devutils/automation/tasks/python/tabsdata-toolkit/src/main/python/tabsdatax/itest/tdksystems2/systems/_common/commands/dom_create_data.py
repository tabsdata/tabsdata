#
# Copyright 2026 Tabsdata Inc.
#

"""Generate and load deterministic initial data for the domain's model."""

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
    cli.add_force(parser)
    cli.add_out(parser)
    cli.add_latch(parser)


def run(ctx: cli.Context) -> int:
    assert isinstance(ctx.info, descriptor.DomainInfo) and ctx.path is not None
    a = ctx.args
    info = ctx.info
    sysobj = ctx.system
    model = info.model
    if not model:
        raise TdxError(f"domain '{ctx.name}' has no model — run create-model first")
    if a.size < 1:
        raise TdxError(f"{constants.OPT_SIZE} must be >= 1 (1 = base)")
    if info.data and not a.force:
        raise TdxError(
            f"domain '{ctx.name}' already has data (size x{info.data}); "
            f"use {constants.OPT_FORCE} to reload"
        )
    uri = sysobj.data_plane_uri(info.meta)
    model_obj = datagen.load_model(model)
    n = datagen.BASE_SIZE * a.size
    tables = model_obj.build_initial(random.Random(datagen.INITIAL_SEED), n)
    if a.force or info.data:
        sysobj.install_schema(info.meta, model, force=True)  # reload = drop + recreate
    entities = sysobj.load_tables(uri, tables)
    descriptor.record_data(ctx.path, str(a.size), entities)
    rows = sum(len(t.rows) for t in tables)
    if a.out == constants.OUT_JSON:
        report = {name: datagen.iud(insert=count) for name, count in entities.items()}
        console.result(json.dumps(report, indent=2))
    else:
        console.good(f"loaded {rows} rows (size x{a.size}) into domain '{ctx.name}'")
    return 0
