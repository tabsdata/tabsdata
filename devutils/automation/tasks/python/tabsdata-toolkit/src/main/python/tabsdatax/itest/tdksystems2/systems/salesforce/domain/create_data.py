#
# Copyright 2026 Tabsdata Inc.
#

"""Replace the org's data with a fresh generated dataset.

Destructive by nature: the load wipes every record of each mapped object before
uploading, because an org's objects are shared state that a previous run has
already filled. There is no schema step, so unlike a SQL domain this may be run
straight after ``create-model``.

``--size`` is accepted (a manifest replay always passes it) but only ``1`` is
valid: the record counts are fixed by the model's snowfakery recipe, so a
multiplier is refused rather than silently ignored.
"""

from __future__ import annotations

import argparse
import json

from tabsdatax.itest.tdksystems2 import cli, console, constants, datagen, descriptor
from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.systems.salesforce.backend import Salesforce


def add_arguments(parser: argparse.ArgumentParser) -> None:
    cli.add_domain(parser)
    cli.add_domain_dir(parser)
    cli.add_size(parser)
    cli.add_force(parser)
    cli.add_out(parser)
    cli.add_latch(parser)


def run(ctx: cli.Context) -> int:
    from tabsdatax.itest.tdksystems2 import salesforce_loader

    assert isinstance(ctx.info, descriptor.DomainInfo) and ctx.path is not None
    a = ctx.args
    info = ctx.info
    model = info.model
    if not model:
        raise TdxError(f"domain '{ctx.name}' has no model — run create-model first")
    if a.size != 1:
        raise TdxError(
            f"{constants.OPT_SIZE} must be 1 for {ctx.slug}: the record counts are "
            f"fixed by model '{model}'s snowfakery recipe"
        )
    if info.data and not a.force:
        raise TdxError(
            f"domain '{ctx.name}' already has data; use {constants.OPT_FORCE} to "
            f"wipe the org and reload"
        )
    assert isinstance(ctx.system, Salesforce)
    entities = salesforce_loader.load(
        ctx.system.model_assets(model),
        info.meta,
        log=console.out,
    )
    descriptor.record_data(ctx.path, "1", entities)
    if a.out == constants.OUT_JSON:
        report = {name: datagen.iud(insert=n) for name, n in entities.items()}
        console.result(json.dumps(report, indent=2))
    else:
        total = sum(entities.values())
        console.good(f"loaded {total} record(s) into domain '{ctx.name}'")
    return 0
