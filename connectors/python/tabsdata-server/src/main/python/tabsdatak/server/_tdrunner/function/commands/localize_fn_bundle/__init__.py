#
# Copyright 2026 Tabs Data Inc.
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
"""

from __future__ import annotations

import argparse
import tarfile
from pathlib import Path

import zstandard

from tabsdatak.server._tdrunner.function._lib.bundle import extract_bundle
from tabsdatak.server._tdrunner.function._lib.cli import build_parser, command
from tabsdatak.server._tdrunner.function._lib.errors import CommandSpecificError
from tabsdatak.server._tdrunner.function._lib.request import load_request_or_fail
from tabsdatak.server._tdrunner.function._lib.transporter_proc import (
    TransporterNotFoundError,
    run_transporter,
)
from tabsdatak.server._tdrunner.function._lib.transporter_request import (
    build_bundle_fetch_request,
    write_transporter_request,
)

VERSION = "0.1.0"
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

# Command-specific exit codes (>= 10 per docs/01-CommandBasics.md).
EXIT_BAD_REQUEST = 10
EXIT_TRANSPORTER_FAILED = 12
EXIT_EXTRACT_FAILED = 13


def _build_parser():
    parser = build_parser(
        prog="localize-fn-bundle",
        description="Fetch and extract the function bundle.",
        version=VERSION,
        info=INFO,
        man=MAN,
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
    return parser


def _fetch_via_transporter(
    request,
    bundle_archive: Path,
    transporter_request: Path,
    transporter_response: Path,
) -> None:
    copy_req = build_bundle_fetch_request(request, bundle_archive)
    write_transporter_request(copy_req, transporter_request)
    try:
        rc = run_transporter(transporter_request, transporter_response)
    except TransporterNotFoundError as exc:
        raise CommandSpecificError(
            EXIT_TRANSPORTER_FAILED, f"td_transporter not found: {exc}"
        ) from exc
    if rc != 0:
        raise CommandSpecificError(
            EXIT_TRANSPORTER_FAILED,
            f"td_transporter exited {rc} fetching bundle",
        )


def _extract(archive: Path, dest_dir: Path) -> None:
    try:
        extract_bundle(archive, dest_dir)
    except (
        tarfile.TarError,
        FileNotFoundError,
        zstandard.ZstdError,
    ) as exc:
        raise CommandSpecificError(
            EXIT_EXTRACT_FAILED, f"failed to extract bundle: {exc}"
        ) from exc


@command(name="td_localize_fn_bundle", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    bundle_dir = Path(args.bundle_dir)
    bundle_archive = Path(args.bundle_archive)
    transporter_request = Path(args.transporter_request)
    transporter_response = Path(args.transporter_response)

    request = load_request_or_fail(request_path, EXIT_BAD_REQUEST)
    _fetch_via_transporter(
        request, bundle_archive, transporter_request, transporter_response
    )
    _extract(bundle_archive, bundle_dir)
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
