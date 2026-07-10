#
# Copyright 2026 Tabs Data Inc.
#

"""run-streamer runner.

The command's logic, callable without argparse or the responder. The
package ``__init__`` wraps :func:`run` in the ``@command`` entrypoint
for the console script; the compound imports :func:`run` directly and
supplies a pre-built ``Namespace``.
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
from tabsdatak.server._tdrunner.function._lib.command import notify_done
from tabsdatak.server._tdrunner.function._lib.connection import load_connection
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


def run(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    bundle_dir = Path(args.bundle_dir)
    out_dir = Path(args.out_dir)
    connection_file = Path(args.connection_file)
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
    conn = load_connection(connection_file)
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
        plugin.run(ctx, conn, info.source)
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
