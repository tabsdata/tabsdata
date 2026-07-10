#
# Copyright 2026 Tabs Data Inc.
#

"""localize-fn-bundle runner.

The command's logic, callable without argparse or the responder. The
package ``__init__`` wraps :func:`run` in the ``@command`` entrypoint
for the console script; the compound imports :func:`run` directly and
supplies a pre-built ``Namespace``.
"""

from __future__ import annotations

import argparse
import logging
import tarfile
from pathlib import Path

import zstandard

from tabsdatak.common._transporter.invoker.transporter1 import (
    TransporterBinaryNotFoundError,
)
from tabsdatak.server._tdrunner.function._lib.bundle import extract_bundle
from tabsdatak.server._tdrunner.function._lib.request import load_any_request_or_raise
from tabsdatak.server._tdrunner.function._lib.transporter import (
    build_bundle_fetch_request,
    run_transporter,
    write_transporter_request,
)
from tabsdatak.server._tdrunner.function.commands.localize_fn_bundle.error import (
    LocalizeFnBundleErrorCode,
)

logger = logging.getLogger(__name__)


def _fetch_via_transporter(
    request,
    bundle_archive: Path,
    transporter_request: Path,
    transporter_response: Path,
    storage_config: Path,
) -> None:
    copy_req = build_bundle_fetch_request(request, bundle_archive, storage_config)
    logger.debug(
        "Bundle fetch transporter request: %d pair(s): %s -> %s",
        len(copy_req.pairs),
        copy_req.pairs,
        transporter_request,
    )
    write_transporter_request(copy_req, transporter_request)
    try:
        rc = run_transporter(transporter_request, transporter_response)
    except TransporterBinaryNotFoundError as exc:
        raise LocalizeFnBundleErrorCode.LFB_2.exception(cause=exc, detail=str(exc))
    if rc != 0:
        raise LocalizeFnBundleErrorCode.LFB_3.exception(rc=rc)
    logger.debug("Bundle archive downloaded to %s", bundle_archive)


def _extract(archive: Path, dest_dir: Path) -> None:
    try:
        extract_bundle(archive, dest_dir)
    except (
        tarfile.TarError,
        FileNotFoundError,
        zstandard.ZstdError,
    ) as exc:
        raise LocalizeFnBundleErrorCode.LFB_4.exception(cause=exc, detail=str(exc))


def run(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    bundle_dir = Path(args.bundle_dir)
    bundle_archive = Path(args.bundle_archive)
    transporter_request = Path(args.transporter_request)
    transporter_response = Path(args.transporter_response)
    storage_config = Path(args.storage_config)

    logger.info("Localizing function bundle")
    request = load_any_request_or_raise(request_path, LocalizeFnBundleErrorCode.LFB_1)
    logger.debug(
        "Bundle URI: %s -> archive %s, extract dir %s",
        request.info.function_bundle.uri,
        bundle_archive,
        bundle_dir,
    )
    _fetch_via_transporter(
        request,
        bundle_archive,
        transporter_request,
        transporter_response,
        storage_config,
    )
    logger.info("Extracting bundle archive into %s", bundle_dir)
    _extract(bundle_archive, bundle_dir)
    logger.info("Function bundle localized")
    return 0
