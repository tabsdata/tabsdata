#
# Copyright 2026 Tabsdata Inc.
#

"""Pull the docker image(s) a system needs.

``--type ALL`` pulls the images for every container system, ``--type a,b`` for
the listed ones; the pulled images are de-duplicated across the selection.
"""

from __future__ import annotations

import argparse

from tabsdatax.itest.tdksystems2 import cli, console, constants, systems
from tabsdatax.itest.tdksystems2.shell import docker_pull, require_docker


def add_arguments(parser: argparse.ArgumentParser) -> None:
    cli.add_type(parser)


def _slugs(ctx: cli.Context) -> list[str]:
    raw = ctx.args.type
    if raw and raw.upper() == constants.TARGET_ALL:
        return [
            s
            for s in systems.all_slugs()
            if getattr(systems.get_class(s), "is_container", False)
        ]
    if raw and "," in raw:
        return [n.strip() for n in raw.split(",") if n.strip()]
    return [ctx.slug]


def run(ctx: cli.Context) -> int:
    require_docker()
    slugs = _slugs(ctx)
    images: dict[str, str] = {}
    for slug in slugs:
        for image, plat in systems.get(slug).docker_images():
            images[image] = plat
    if not images:
        console.out("no docker images to fetch for the selected type(s)")
        return 0
    for image, plat in images.items():
        console.out(f">>> pulling {image} …")
        docker_pull(image, plat)
    console.good(f"fetched images for {', '.join(slugs)}")
    return 0
