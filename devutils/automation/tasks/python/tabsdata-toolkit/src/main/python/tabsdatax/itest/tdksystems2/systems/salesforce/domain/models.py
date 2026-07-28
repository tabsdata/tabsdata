#
# Copyright 2026 Tabsdata Inc.
#

"""List the models the org can load.

The shared implementation describes each model through its datagenerator (which
entities grow with ``--size``, which delta ops they support); a salesforce model
has none, so this reports the entities it uploads and the fixed record counts its
recipe produces.
"""

from __future__ import annotations

import argparse

from tabsdatax.itest.tdksystems2 import cli, console, datagen


def add_arguments(parser: argparse.ArgumentParser) -> None:
    cli.add_domain(parser)
    cli.add_domain_dir(parser)


def run(ctx: cli.Context) -> int:
    avail = ctx.system.available_models()
    if not avail:
        console.out("(no models available for this system)")
        return 0
    console.out("Available models  (record counts are fixed by each recipe)")
    for model in avail:
        console.out(f"  {model}")
        for name in datagen.entities_of(model):
            console.out(f"    {name}")
    return 0
