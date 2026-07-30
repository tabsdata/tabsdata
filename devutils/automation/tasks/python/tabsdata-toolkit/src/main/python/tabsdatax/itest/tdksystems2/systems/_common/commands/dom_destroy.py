#
# Copyright 2026 Tabsdata Inc.
#

"""Drop a domain (its entity-container) from its system."""

from __future__ import annotations

import argparse

from tabsdatax.itest.tdksystems2 import cli, console


def add_arguments(parser: argparse.ArgumentParser) -> None:
    cli.add_domain(parser)
    cli.add_domain_dir(parser)
    cli.add_force(parser)
    cli.add_dry_run(parser)
    cli.add_latch(parser)
    cli.add_manifest(parser, "destroy")


def run(ctx: cli.Context) -> int:
    assert ctx.info is not None and ctx.path is not None
    if ctx.args.dry_run:
        console.out(f"would destroy domain '{ctx.name}'")
        return 0
    console.out(f"▶ destroying domain '{ctx.name}' …")
    try:
        ctx.system.destroy_domain(ctx.info.meta)
    except Exception as exc:  # noqa: BLE001 - -f removes the descriptor regardless
        if not ctx.args.force:
            raise
        console.warn(f"teardown failed ({exc}); -f: removing descriptor anyway")
    ctx.path.unlink(missing_ok=True)
    console.good(f"destroyed domain '{ctx.name}'")
    return 0
