#
# Copyright 2026 Tabsdata Inc.
#

"""localize-fn-bundle command.

Step 3 of the publisher init-container chain. Reads the bundle URI from
request.yaml, fetches the tarball via `td_transporter`, and extracts it
into ``--bundle-dir``. See `docs/02-PublisherInitContainers.md`.

Bundle-fetch protocol per Q-2.3 (option a): we shell out to
`td_transporter` rather than doing HTTP/cloud-SDK fetches in Python — the
binary already understands every URI scheme our Locations use.

The pod-runner activates the venv before invoking us (per
`02-PublisherInitContainers.md` "Venv activation: pod-runner
responsibility"), so this command does not take `--run-env`, does not
parse run-env.yaml, and does not spawn any subprocess of the venv itself.

The logic lives in :mod:`.runner`; this module is the thin ``@command``
entrypoint the console script resolves to. The compound imports
:func:`.runner.run` directly instead of going through ``main``.
"""

from __future__ import annotations

import argparse

from tabsdatak.server._tdrunner.function._commons.run.command import (
    build_parser,
    command,
)
from tabsdatak.server._tdrunner.function.commands.localize_fn_bundle.runner import (
    _extract,
    _fetch_via_transporter,
    run,
)

__all__ = ["_extract", "_fetch_via_transporter", "run", "main"]

VERSION = "2.2.1"
INFO = (
    "localize-fn-bundle — fetch the function bundle via td_transporter and extract it"
)
MAN = (
    "localize-fn-bundle --request <PATH> --bundle-dir <PATH> "
    "--bundle-archive <PATH> --transporter-request <PATH> "
    "--transporter-response <PATH> --liveness <PATH>\n\n"
    "Reads the bundle URI from --request, downloads the tarball to "
    "--bundle-archive via td_transporter (using --transporter-request "
    "and --transporter-response), and extracts the archive into "
    "--bundle-dir. The caller owns the lifecycle of every path. Step "
    "3 of the publisher init-container chain."
)


def _build_parser():
    parser = build_parser(
        program="localize-fn-bundle",
        description="Fetch and extract the function bundle.",
        version=VERSION,
        info=INFO,
        manual=MAN,
    )
    parser.add_argument("--request", required=True, help="Path to request.yaml")
    parser.add_argument(
        "--bundle-dir",
        required=True,
        help="Destination dir for bundle extraction.",
    )
    parser.add_argument(
        "--bundle-archive",
        required=True,
        help="Path to the downloaded bundle archive (e.g. bundle.tar.zst).",
    )
    parser.add_argument(
        "--transporter-request",
        required=True,
        help="Path to write the transporter request yaml.",
    )
    parser.add_argument(
        "--transporter-response",
        required=True,
        help="Path to write the transporter response yaml.",
    )
    parser.add_argument(
        "--storage-config",
        required=True,
        dest="storage_config",
        help="Path to the storage configuration file.",
    )
    return parser


@command(name="td_localize_fn_bundle", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    return run(args)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
