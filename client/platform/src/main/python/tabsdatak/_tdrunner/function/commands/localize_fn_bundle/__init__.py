#
# Copyright 2026 Tabs Data Inc.
#

"""localize-fn-bundle command.

Step 3 of the publisher init-container chain. Reads the bundle URI from
request.yaml, fetches the tarball via `td_transporter`, and extracts it
into ``--dir``. See `docs/02-PublisherInitContainers.md`.

Bundle-fetch protocol per Q-2.3 (option a): we shell out to
`td_transporter` rather than doing HTTP/cloud-SDK fetches in Python — the
binary already understands every URI scheme our Locations use.
"""

from __future__ import annotations

import tarfile
import tempfile
from pathlib import Path

from tabsdatak._tdrunner.function._lib.bundle import extract_bundle
from tabsdatak._tdrunner.function._lib.cli import build_parser, run_command
from tabsdatak._tdrunner.function._lib.errors import CommandSpecificError
from tabsdatak._tdrunner.function._lib.liveness import Liveness
from tabsdatak._tdrunner.function._lib.request import (
    InvalidRequestError,
    load_request,
)
from tabsdatak._tdrunner.function._lib.run_env import (
    InvalidRunEnvError,
    load_run_env,
)
from tabsdatak._tdrunner.function._lib.transporter_proc import (
    TransporterNotFoundError,
    run_transporter,
)
from tabsdatak._tdrunner.function._lib.transporter_request import (
    build_bundle_fetch_request,
    write_transporter_request,
)
from tabsdatak._tdrunner.function.commands import tabsdata_command

VERSION = "0.1.0"
INFO = (
    "localize-fn-bundle — fetch the function bundle via td_transporter and extract it"
)
MAN = (
    "localize-fn-bundle --request <PATH> --run-env <PATH> --dir <PATH> "
    "--liveness <PATH>\n\n"
    "Reads the bundle URI from --request, downloads the tarball via "
    "td_transporter, and extracts it to --dir. Step 3 of the publisher "
    "init-container chain."
)

# Command-specific exit codes (>= 10 per docs/01-CommandBasics.md).
EXIT_BAD_REQUEST = 10
EXIT_BAD_RUN_ENV = 11
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
        "--run-env",
        required=True,
        dest="run_env",
        help="Path to run-env.yaml",
    )
    parser.add_argument("--dir", required=True, help="Destination dir for extraction")
    return parser


def _load_request(path: Path):
    try:
        return load_request(path)
    except (FileNotFoundError, InvalidRequestError) as exc:
        raise CommandSpecificError(
            EXIT_BAD_REQUEST, f"unreadable request.yaml: {exc}"
        ) from exc


def _load_run_env(path: Path) -> None:
    try:
        load_run_env(path)
    except (FileNotFoundError, InvalidRunEnvError) as exc:
        raise CommandSpecificError(
            EXIT_BAD_RUN_ENV, f"unreadable run-env.yaml: {exc}"
        ) from exc


def _fetch_via_transporter(request, target: Path, transporter_yaml: Path) -> None:
    copy_req = build_bundle_fetch_request(request, target)
    write_transporter_request(copy_req, transporter_yaml)
    try:
        rc = run_transporter(transporter_yaml)
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
    except (tarfile.TarError, FileNotFoundError) as exc:
        raise CommandSpecificError(
            EXIT_EXTRACT_FAILED, f"failed to extract bundle: {exc}"
        ) from exc


def _run() -> int:
    args = _build_parser().parse_args()
    request_path = Path(args.request)
    run_env_path = Path(args.run_env)
    dest_dir = Path(args.dir)
    liveness_path = Path(args.liveness)

    with Liveness(liveness_path):
        request = _load_request(request_path)
        _load_run_env(run_env_path)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            bundle_local = tmp_dir / "bundle.tar.gz"
            transporter_yaml = tmp_dir / "transporter.yaml"
            _fetch_via_transporter(request, bundle_local, transporter_yaml)
            _extract(bundle_local, dest_dir)
    return 0


@tabsdata_command(name="td_localize_fn_bundle")
def main() -> int:
    return run_command(_run)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
