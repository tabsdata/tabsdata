#
# Copyright 2026 Tabsdata Inc.
#

"""Report a domain's reachability."""

from __future__ import annotations

import argparse

from tabsdatax.itest.tdksystems2 import cli, console


def add_arguments(parser: argparse.ArgumentParser) -> None:
    cli.add_domain(parser)
    cli.add_domain_dir(parser)


def run(ctx: cli.Context) -> int:
    assert ctx.info is not None
    ok, detail = ctx.system.domain_status(ctx.info.meta)
    (console.good if ok else console.bad)(f"{ctx.name}: {detail}")
    return 0 if ok else 1
