#
# Copyright 2026 Tabsdata Inc.
#

"""run-source-plugin runner.

The command's logic, callable without argparse or the responder. The
package ``__init__`` wraps :func:`run` in the ``@command`` entrypoint
for the console script; the compound imports :func:`run` directly and
supplies a pre-built ``Namespace``.
"""

from __future__ import annotations

import argparse
import inspect
import logging
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlparse
from urllib.request import url2pathname

from tabsdatak._connectors import get_src, get_stream_src
from tabsdatak._info import (
    KIND_PUBLISHER_DEF,
    KIND_STREAM_PUBLISHER_DEF,
    PublisherInfo,
    StreamPublisherInfo,
    _unwrap_info,
)
from tabsdatak.server._tdrunner.function._lib.connection import load_connection
from tabsdatak.server._tdrunner.function._lib.endpoint import UnsupportedSchemeError
from tabsdatak.server._tdrunner.function._lib.function_def import (
    load_function_def_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.initial_values import (
    drain_ctx_to_state_or_raise,
    hydrate_ctx_from_state_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.inputs_manifest import (
    build_entry,
    build_table_record,
    write_inputs_manifest,
)
from tabsdatak.server._tdrunner.function._lib.request import (
    Location,
    load_request_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.request_ctx import FromRequestCtx
from tabsdatak.server._tdrunner.function._lib.stage_backend import (
    stage_backend_from_location,
)
from tabsdatak.server._tdrunner.function.commands.run_source_plugin.error import (
    RunSourcePluginErrorCode,
)
from tabsdatak.spi import SrcPluginCtx, TableFileInput

logger = logging.getLogger(__name__)


def _load_function_def(bundle_dir: Path) -> PublisherInfo | StreamPublisherInfo:
    """Load the function definition from the bundle -- thin wrapper
    over :func:`load_function_def_or_raise` pinning the accepted
    kind set and the command's per-site error codes.

    Accepts both ``publisherDef`` (regular ``@publisher``) and
    ``streamPublisherDef`` (``@stream_publisher`` -- runs the
    substituted manifest-stage source on the publisher side; see
    module docstring).
    """
    return load_function_def_or_raise(
        bundle_dir,
        allowed_kinds={
            KIND_PUBLISHER_DEF: PublisherInfo,
            KIND_STREAM_PUBLISHER_DEF: StreamPublisherInfo,
        },
        bad_bundle_code=RunSourcePluginErrorCode.RSP_2,
        bad_user_code_code=RunSourcePluginErrorCode.RSP_3,
        wrong_kind_code=RunSourcePluginErrorCode.RSP_4,
    )


class _SrcContext(FromRequestCtx, SrcPluginCtx):
    """Concrete :class:`SrcPluginCtx` for the run-source-plugin command.

    All behaviour comes from :class:`FromRequestCtx`; this class
    only fixes the abstract base so ``isinstance(ctx, SrcPluginCtx)``
    holds at the call boundary.
    """


def _build_manifest_stage_conn(function_stage: Location) -> SimpleNamespace:
    """Build the duck-typed ``ManifestStageConn`` the substituted
    ``ManifestStageSrcPlugin`` reads via ``backend_for_conn``.

    The streamer pod wrote its manifests + parquets into
    ``function_stage`` (under ``manifests/`` and ``files/`` subdirs
    respectively; see :mod:`stage_layout`). The publisher pod needs a
    conn pointing at the *stage root* so the plugin can glob
    ``manifests/stream-manifest-*.json`` and resolve each manifest's
    inner ``files/<TS>/<name>.parquet`` entries (one files subdir per
    manifest).

    ``file://`` keeps the connector-conn form (a ``LocalFileSrcConn`` in
    the ``local`` slot). Cloud schemes (``s3://`` / ``az://`` / ``gs://``)
    are resolved to a :class:`StageBackend` via
    :func:`stage_backend_from_location` -- which reuses :mod:`endpoint`'s
    ``env_prefix``-derived credential mapping -- and carried on the
    ``stage_backend`` attribute, which ``backend_for_conn`` returns as-is.
    An unknown scheme surfaces the typed ``RSP_11`` so callers get a clean
    exit code rather than an ``AttributeError`` downstream.
    """
    parsed = urlparse(function_stage.uri)
    if parsed.scheme == "file":
        from tabsdatak.conn.localfile import LocalFileSrcConn

        base_path = url2pathname(parsed.path)
        return SimpleNamespace(
            stage_backend=None,
            local=LocalFileSrcConn(base_path=base_path),
            s3=None,
            az=None,
            gcs=None,
        )
    try:
        backend = stage_backend_from_location(function_stage)
    except UnsupportedSchemeError as exc:
        raise RunSourcePluginErrorCode.RSP_11.exception(
            uri=function_stage.uri, scheme=parsed.scheme
        ) from exc
    return SimpleNamespace(
        stage_backend=backend,
        local=None,
        s3=None,
        az=None,
        gcs=None,
    )


def _resolve_plugin_and_io(
    info: PublisherInfo | StreamPublisherInfo,
    function_stage: Location,
    connection_file: Path,
) -> tuple[object, object, object, int]:
    """Pick the right (plugin, conn, src, cardinality) tuple for ``info``.

    For ``PublisherInfo`` the connection is the one in
    ``connection_file``; the source and plugin come from the
    registered ``SrcDef``.

    For ``StreamPublisherInfo`` we substitute (see module docstring):
      * source ← ``StreamSrcDef.publisher_src()`` (an empty marker
        instance, typically ``StreamStageSrc``)
      * conn ← :func:`_build_manifest_stage_conn` against
        ``info.function_stage``
      * plugin ← ``SrcDef.plugin()`` registered for the substituted
        source class

    ``cardinality`` is the slot count for the source the plugin will
    actually run against (the substituted ``StreamStageSrc`` has
    dynamic ``-1`` cardinality; the publisher will pad to the
    user-function-declared count in :func:`_write_inputs_manifest`).

    Raises:
        NoPluginError (RSP_5): the (stream) source class has no
            registered SrcDef / StreamSrcDef.
        UnsupportedStageSchemeError (RSP_11): the stream-publisher's
            ``function_stage`` URI uses a scheme the runtime does not
            yet support.
    """
    if isinstance(info, StreamPublisherInfo):
        try:
            stream_src_def = get_stream_src(type(info.source))
        except KeyError as exc:
            raise RunSourcePluginErrorCode.RSP_5.exception(
                cause=exc,
                src_type=type(info.source).__name__,
                detail=str(exc),
            )
        publisher_src_class = stream_src_def.publisher_src
        try:
            src_def = get_src(publisher_src_class)
        except KeyError as exc:
            raise RunSourcePluginErrorCode.RSP_5.exception(
                cause=exc,
                src_type=publisher_src_class.__name__,
                detail=str(exc),
            )
        substituted_conn = _build_manifest_stage_conn(function_stage)
        substituted_src = publisher_src_class()
        return (
            src_def.plugin(),
            substituted_conn,
            substituted_src,
            src_def.cardinality(substituted_src),
        )

    try:
        src_def = get_src(type(info.source))
    except KeyError as exc:
        raise RunSourcePluginErrorCode.RSP_5.exception(
            cause=exc, src_type=type(info.source).__name__, detail=str(exc)
        )
    conn = load_connection(connection_file)
    return src_def.plugin(), conn, info.source, src_def.cardinality(info.source)


def _invoke_read_in(plugin, ctx: SrcPluginCtx, conn, src) -> list[list[TableFileInput]]:
    try:
        return plugin.read_in(ctx, conn, src)
    except Exception as exc:
        raise RunSourcePluginErrorCode.RSP_6.exception(cause=exc, detail=str(exc))


def _normalise_table_file(p) -> str:
    """Render a `TableFileSpec` as an absolute filesystem path string.

    `TableFileSpec` is declared as `ParquetFile` (a `Path` subclass),
    which should mean a real filesystem path. In practice some
    connector plugins (e.g. `LocalFileSrcPlugin`) return the path
    parsed verbatim from the transporter report's `to:` field — i.e.
    `Path("file:///...")` (a URI wrapped in `Path`, not an actual
    filesystem path). `Path.absolute()` on such a value mistakes the
    `file:` prefix for a path segment and prepends the CWD.

    Accept both shapes here so the manifest always contains a clean
    absolute filesystem path.

    TODO: clarify the `TableFileSpec` contract and unwrap at the
    plugin layer; this normalisation should then become unnecessary.
    """
    s = str(p)
    if s[:5].lower().startswith("file:"):
        # Manifest paths are posix-canonical (portable, stable across OS).
        # `url2pathname` gives the native path; `as_posix` re-canonicalizes
        # a Windows `C:\a\b` to `C:/a/b` and leaves posix paths unchanged.
        return Path(url2pathname(urlparse(s).path)).as_posix()
    return Path(s).absolute().as_posix()


def _write_inputs_manifest(
    info: PublisherInfo | StreamPublisherInfo,
    produced: list[list[TableFileInput]],
    path: Path,
    *,
    cardinality: int,
) -> None:
    """Write the YAML inputs-manifest to `path`.

    The manifest maps each *function input parameter* (in declaration
    order) to the parquet files the source plugin produced for it.
    Input parameters are unrelated to `info.output_tables` — the
    output-tables list describes what the function *returns*, not what
    feeds in to it.

    ``cardinality`` is the *resolved* slot count for the source the
    plugin actually ran against (the user-declared source for
    ``PublisherInfo``, or the substituted ``publisher_src`` for
    ``StreamPublisherInfo``) -- :func:`_resolve_plugin_and_io` is the
    single source of truth for this value.

    A negative ``cardinality`` (the ``-1`` sentinel) means the source's
    slot count is decided dynamically at read time -- e.g. a
    stream-stage manifest source, whose slot count comes from the
    manifest, not from static config. In that case the manifest is
    padded up to the user function's declared input-slot count with
    empty entries: the function is *always* invoked with exactly its
    declared signature, and a stream-publisher that fired against no
    staged manifest reaches the user function with one empty slot per
    declared parameter (e.g. ``stream_people(t)`` receives ``t=[]`` --
    the downstream ``_materialize_input_slot`` materialises empty
    slots as ``[]`` for ARRAY mode / ``None`` for SINGLE mode).
    Over-production (more slots than declared) is still a contract
    violation.
    """
    func = _unwrap_info(info.func)
    parameters = list(inspect.signature(func).parameters)

    if cardinality < 0:
        # Dynamic source: pad up to the function-declared slot count
        # with empty inner lists when the plugin returned fewer slots.
        # Over-production is still a violation (we can't index past
        # parameters[i]).
        expected = len(parameters)
        if len(produced) > expected:
            raise RunSourcePluginErrorCode.RSP_7.exception(
                returned=len(produced), expected=expected
            )
        if len(produced) < expected:
            produced = list(produced) + [[] for _ in range(expected - len(produced))]
        cardinality = expected
    elif len(produced) != cardinality:
        raise RunSourcePluginErrorCode.RSP_7.exception(
            returned=len(produced), expected=cardinality
        )

    entries = [
        build_entry(
            name=parameters[i],
            table_pos=i,
            tables=[
                build_table_record(
                    file=_normalise_table_file(f.file),
                    input_idx=None,
                    version_pos=-1,
                )
                for f in produced[i]
            ],
        )
        for i in range(cardinality)
    ]

    try:
        write_inputs_manifest(entries, path)
    except OSError as exc:
        raise RunSourcePluginErrorCode.RSP_8.exception(
            cause=exc, path=path, detail=str(exc)
        )


def run(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    bundle_dir = Path(args.bundle_dir)
    out_dir = Path(args.out_dir)
    inputs_manifest = Path(args.inputs_manifest)
    state_dir = Path(args.state_dir)
    connection_file = Path(args.connection_file)

    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Running source plugin")
    request = load_request_or_raise(request_path, RunSourcePluginErrorCode.RSP_1)
    info = _load_function_def(bundle_dir)
    plugin, conn, src, cardinality = _resolve_plugin_and_io(
        info, request.info.function_stage, connection_file
    )
    logger.debug(
        "Resolved Src plugin %s for connector %s (kind=%s, cardinality=%d)",
        type(plugin).__name__,
        type(conn).__name__,
        type(info).__name__,
        cardinality,
    )
    ctx = _SrcContext(
        work_dir=out_dir,
        exec_plan_id=str(request.info.execution_id),
        trx_id=str(request.info.transaction_id),
        exec_plan_ts=request.info.triggered_on_iso,
        collection_id=str(request.info.collection_id),
        collection_name=request.info.collection,
        function_id=str(request.info.function_run_id),
        function_name=request.info.function,
    )
    hydrate_ctx_from_state_or_raise(
        ctx, request, state_dir, error_code=RunSourcePluginErrorCode.RSP_9
    )
    logger.debug("Hydrated SrcPluginCtx from %s; work_dir=%s", state_dir, out_dir)

    logger.info("Invoking source plugin's read_in")
    produced = _invoke_read_in(plugin, ctx, conn, src)
    logger.info("Source plugin produced %d slot(s)", len(produced))

    drain_ctx_to_state_or_raise(
        ctx, request, state_dir, error_code=RunSourcePluginErrorCode.RSP_10
    )
    logger.debug("Drained ctx scratch to %s", state_dir)

    _write_inputs_manifest(info, produced, inputs_manifest, cardinality=cardinality)
    logger.info("Wrote raw inputs manifest to %s", inputs_manifest)
    return 0
