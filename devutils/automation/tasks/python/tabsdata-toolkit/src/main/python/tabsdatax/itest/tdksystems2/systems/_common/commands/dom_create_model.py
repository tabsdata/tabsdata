#
# Copyright 2026 Tabsdata Inc.
#

"""Install a model's schema (DDL) into a domain."""

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
    sysobj = ctx.system
    model = a.model
    had_model = bool(info.model)
    if had_model and not a.force:
        raise TdxError(
            f"domain '{ctx.name}' already has model '{info.model}' "
            f"(use {constants.OPT_FORCE} to replace)"
        )
    if not sysobj.has_model(model):
        avail = ", ".join(sysobj.available_models()) or "none"
        raise TdxError(
            f"system '{ctx.slug}' has no DDL for model '{model}' (available: {avail})"
        )
    sysobj.install_schema(info.meta, model, force=a.force or had_model)
    entity_names = datagen.entities_of(model)
    descriptor.set_model(ctx.path, model, entity_names)
    if a.out == constants.OUT_JSON:
        report = {name: datagen.iud() for name in entity_names}
        console.result(json.dumps(report, indent=2))
    else:
        console.good(f"installed model '{model}' into domain '{ctx.name}'")
    return 0
