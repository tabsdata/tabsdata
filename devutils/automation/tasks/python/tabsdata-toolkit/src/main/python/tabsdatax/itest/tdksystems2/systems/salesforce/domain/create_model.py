#
# Copyright 2026 Tabsdata Inc.
#

"""Record the model a salesforce domain will load.

Account and Opportunity are standard objects that every org already has, so
unlike a SQL domain there is no schema to install — this only validates the model
and stamps it on the descriptor. The records arrive with ``create-data``, whose
first step wipes whatever is in the org.
"""

from __future__ import annotations

import argparse
import json

from tabsdatax.itest.tdksystems2 import cli, console, constants, datagen, descriptor
from tabsdatax.itest.tdksystems2.errors import TdxError


def add_arguments(parser: argparse.ArgumentParser) -> None:
    cli.add_domain(parser)
    cli.add_domain_dir(parser)
    cli.add_model(parser)
    cli.add_force(parser)
    cli.add_out(parser)
    cli.add_latch(parser)


def run(ctx: cli.Context) -> int:
    assert isinstance(ctx.info, descriptor.DomainInfo) and ctx.path is not None
    a = ctx.args
    info = ctx.info
    model = a.model
    if info.model and not a.force:
        raise TdxError(
            f"domain '{ctx.name}' already has model '{info.model}' "
            f"(use {constants.OPT_FORCE} to replace)"
        )
    if not ctx.system.has_model(model):
        avail = ", ".join(ctx.system.available_models()) or "none"
        raise TdxError(
            f"system '{ctx.slug}' has no recipe for model '{model}' "
            f"(available: {avail})"
        )
    entity_names = datagen.entities_of(model)
    descriptor.set_model(ctx.path, model, entity_names)
    if a.out == constants.OUT_JSON:
        console.result(json.dumps({n: datagen.iud() for n in entity_names}, indent=2))
    else:
        console.good(
            f"recorded model '{model}' on domain '{ctx.name}' "
            f"(the org's objects already exist; run create-data to load it)"
        )
    return 0
