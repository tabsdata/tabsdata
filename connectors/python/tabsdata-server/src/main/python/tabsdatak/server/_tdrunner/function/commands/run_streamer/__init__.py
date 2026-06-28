#
# Copyright 2026 Tabs Data Inc.
#

"""run-streamer command.

Long-running entrypoint of the streamer pod's main container. Loads
the ``StreamPublisherInfo`` from the extracted bundle, builds a
:class:`StreamerPluginCtx`, and invokes the
:class:`StreamSrcPlugin`'s ``run(ctx, conn, stream_src)`` until the
pod receives SIGTERM / SIGINT or the plugin returns of its own accord.

The plugin's responsibility is to honour ``ctx.active`` (poll loop
exits when it flips to ``False``) and to call ``ctx.stage(...)`` once
per rollover. Everything else -- writing the manifest under
``<stage>/manifests/``, moving parquets into ``<stage>/files/``,
persisting the envelope at ``<stage>/stream-state.json`` -- is the
ctx's job (see :mod:`stage_layout` for the layout convention and
:class:`StreamerPluginCtx` for the IO).

Pod chain (see ``stream-publisher_kminus.yaml``):

  * ``0010-begin-execution`` -- initialise journal + notify_running
  * ``0020-fetch-request`` -- pull request.yaml from apiserver
  * ``0030-localize-fn-bundle`` -- download + extract the user bundle
  * ``0900-manifest-trigger`` -- sidecar (NU7); scans
    ``<stage>/manifests/`` and POSTs ``fn_trigger`` per manifest
  * ``1000-run-streamer`` (this command) -- long-running, exits 0 on
    clean shutdown via SIGTERM
"""

from __future__ import annotations

import argparse
import logging
import signal
from datetime import datetime, timezone
from pathlib import Path

from tabsdatak._connectors import get_stream_src
from tabsdatak._info import KIND_STREAM_PUBLISHER_DEF, StreamPublisherInfo, _unwrap_info
from tabsdatak.extensions.variant.version import version as _tabsdata_version
from tabsdatak.server._tdrunner.function._lib.command import (
    build_parser,
    command,
    notify_done,
)
from tabsdatak.server._tdrunner.function._lib.endpoint import (
    UnsupportedSchemeError as _StageUnsupportedSchemeError,
)
from tabsdatak.server._tdrunner.function._lib.function_def import (
    load_function_def_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.journal import Journal
from tabsdatak.server._tdrunner.function._lib.request import (
    load_streamer_request_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.stream_src_ctx import (
    StreamerPluginCtx,
)
from tabsdatak.server._tdrunner.function.commands.run_streamer.error import (
    RunStreamerErrorCode,
)

logger = logging.getLogger(__name__)

VERSION = "2.0.0"
INFO = (
    "run-streamer — long-running streamer pod entrypoint; runs the "
    "@stream_publisher's StreamSrcPlugin until SIGTERM"
)
MAN = (
    "run-streamer\n"
    "    --request <PATH>\n"
    "    --bundle-dir <PATH>\n"
    "    --out-dir <PATH>\n"
    "    --shared-dir <PATH>\n"
    "    --work-dir <PATH>\n"
    "    --log-config <PATH>\n"
    "    --liveness <PATH>\n"
    "    --responder-url <URL>\n"
    "    --responder-method <METHOD>\n"
    "    --responder-authorization <AUTH>\n"
    "    --journal-file <PATH>\n\n"
    "Loads the StreamPublisherInfo from --bundle-dir, builds the "
    "StreamerPluginCtx from request.info, installs SIGTERM/SIGINT "
    "handlers that flip ctx.active to False, then invokes "
    "plugin.run(ctx, conn, stream_src) and blocks until it returns. "
    "Stamps journal.end + posts Done on clean exit."
)


def _build_parser():
    parser = build_parser(
        prog="run-streamer",
        description="Long-running streamer pod entrypoint.",
        version=VERSION,
        info=INFO,
        man=MAN,
    )
    parser.add_argument("--request", required=True, help="Path to request.yaml")
    parser.add_argument(
        "--bundle-dir",
        required=True,
        help="Extracted bundle directory (function_def.json + user code)",
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help=(
            "Local scratch directory the StreamSrcPlugin writes parquets "
            "into; ctx.stage() then moves them to <stage>/files/."
        ),
    )
    return parser


def _load_function_def(bundle_dir: Path) -> StreamPublisherInfo:
    """Streamer-pod-only loader: accepts only ``streamPublisherDef``.
    A regular ``@publisher`` would surface as RS_4 here -- the streamer
    pod is meaningful for stream publishers only."""
    return load_function_def_or_raise(
        bundle_dir,
        allowed_kinds={KIND_STREAM_PUBLISHER_DEF: StreamPublisherInfo},
        bad_bundle_code=RunStreamerErrorCode.RS_2,
        bad_user_code_code=RunStreamerErrorCode.RS_3,
        wrong_kind_code=RunStreamerErrorCode.RS_4,
    )


def _resolve_plugin(info: StreamPublisherInfo):
    """Look up the registered ``StreamSrcDef`` for the user-declared
    stream source and instantiate its plugin."""
    try:
        stream_src_def = get_stream_src(type(info.source))
    except KeyError as exc:
        raise RunStreamerErrorCode.RS_5.exception(
            cause=exc, src_type=type(info.source).__name__, detail=str(exc)
        )
    return stream_src_def.plugin()


@command(name="td_run_streamer", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    bundle_dir = Path(args.bundle_dir)
    out_dir = Path(args.out_dir)
    # The streamer plugin writes parquets directly into ctx.work_dir
    # (== --out-dir) before staging them; the runner owns ensuring it
    # exists. Same idempotent mkdir `run_function` does for its own
    # out_dir (`run_function/__init__.py:488`); a missing dir would
    # surface to the user as a confusing `FileNotFoundError` from
    # polars during the plugin's `write_parquet` call.
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Starting streamer pod")
    request = load_streamer_request_or_raise(request_path, RunStreamerErrorCode.RS_1)
    info = _load_function_def(bundle_dir)
    plugin = _resolve_plugin(info)
    # `info.func` is the @stream_publisher-decorated descriptor (the
    # decorator replaces the module-level function binding), not the
    # bare user callable. `_unwrap_info` descends through the wrapper
    # to reach the underlying function so its `__name__` is readable.
    # Same trick `run_function` uses (`_unwrap_info(info.func)` at
    # invocation time).
    user_func = _unwrap_info(info.func)
    logger.debug(
        "Loaded StreamPublisherInfo (function=%s, source=%s); resolved plugin=%s",
        getattr(user_func, "__name__", repr(user_func)),
        type(info.source).__name__,
        type(plugin).__name__,
    )

    function_stage = request.info.function_stage
    try:
        ctx = StreamerPluginCtx(
            work_dir=out_dir,
            collection_id=str(request.info.collection_id),
            collection_name=request.info.collection,
            function_id=str(request.info.function_version_id),
            function_name=request.info.function,
            function_stage=function_stage,
            flush_interval_mins=info.fallback_rollover_mins,
            tabsdata_version=_tabsdata_version(),
        )
    except _StageUnsupportedSchemeError as exc:
        from urllib.parse import urlparse

        raise RunStreamerErrorCode.RS_7.exception(
            cause=exc,
            uri=function_stage.uri,
            scheme=urlparse(function_stage.uri).scheme,
        )

    def _shutdown(signum, _frame):
        logger.info(
            "received signal %d; flipping ctx.active to False so the "
            "plugin loop exits at its next active check",
            signum,
        )
        ctx.mark_inactive()

    try:
        signal.signal(signal.SIGTERM, _shutdown)
        signal.signal(signal.SIGINT, _shutdown)
    except ValueError:
        # Off the main thread (test contexts): signal install isn't
        # supported; the plugin's own ``ctx.active`` will be flipped
        # by the test driver in lieu of a signal.
        logger.debug("not on main thread; skipping signal handler install")

    logger.info(
        "Invoking %s.run() (function=%s, work_dir=%s, stage=%s, "
        "flush_interval_mins=%d)",
        type(plugin).__name__,
        getattr(user_func, "__name__", repr(user_func)),
        out_dir,
        function_stage.uri,
        info.fallback_rollover_mins,
    )
    try:
        plugin.run(ctx, info.conn, info.source)
    except Exception as exc:
        raise RunStreamerErrorCode.RS_6.exception(cause=exc, detail=str(exc))
    logger.info("Streamer plugin returned cleanly")

    end = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    journal_path = Path(args.journal_file)
    journal = Journal.load(journal_path, strict=True)
    journal.end = end
    try:
        journal.save(journal_path)
    except OSError as exc:
        raise RunStreamerErrorCode.RS_8.exception(
            cause=exc, path=journal_path, detail=str(exc)
        )
    logger.info("run-streamer: recorded end=%s", end)

    notify_done(args)
    logger.info("Reported streamer completion to apiserver successfully")
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
