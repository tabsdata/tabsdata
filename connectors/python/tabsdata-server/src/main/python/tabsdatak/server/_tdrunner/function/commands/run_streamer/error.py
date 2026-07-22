#
# Copyright 2026 Tabsdata Inc.
#

"""Error model for the run-streamer command (exit codes 70..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    BadBundleError as _BadBundleError,
    BadRequestError as _BadRequestError,
    BadUserCodeError as _BadUserCodeError,
    CommandRuntimeException,
    NoPluginError as _NoPluginError,
    PluginFailedError as _PluginFailedError,
    WriteFailedError as _WriteFailedError,
    WrongKindError as _WrongKindError,
)


class BadRequestError(_BadRequestError):
    exit_code = 70


class BadBundleError(_BadBundleError):
    """``function_def.json`` missing, unreadable, or malformed."""

    exit_code = 71


class BadUserCodeError(_BadUserCodeError):
    exit_code = 72


class WrongKindError(_WrongKindError):
    """Bundle's ``kind`` isn't ``streamPublisherDef`` -- the streamer
    pod is only meaningful for ``@stream_publisher`` functions."""

    exit_code = 73


class NoPluginError(_NoPluginError):
    """No ``StreamSrcPlugin`` is registered for the resolved
    ``StreamSrc`` class."""

    exit_code = 74


class PluginFailedError(_PluginFailedError):
    """The plugin's ``run()`` raised. The streamer's main loop is the
    plugin; an unhandled exception there is a contract violation that
    surfaces here so the pod exits non-zero and the apiserver can
    decide whether to restart."""

    exit_code = 75


class UnsupportedStageSchemeError(CommandRuntimeException):
    """``info.function_stage.uri`` uses a scheme the streamer runtime
    does not yet support (today: only ``file://``)."""

    exit_code = 76


class WriteFailedError(_WriteFailedError):
    """Journal file could not be written."""

    exit_code = 77


class RunStreamerErrorCode(ErrorCode):
    RS_1 = ErrorDef(
        BadRequestError,
        "unreadable request.yaml {path}: {detail}",
    )
    # ``load_request_or_raise``.

    RS_2 = ErrorDef(
        BadBundleError,
        "could not load function_def.json from {bundle_dir}: {detail}",
    )
    # ``load_function_def_or_raise`` bundle-shape failure.

    RS_3 = ErrorDef(
        BadUserCodeError,
        "could not resolve user function from {bundle_dir}: {detail}",
    )
    # ``load_function_def_or_raise`` user-code failure.

    RS_4 = ErrorDef(
        WrongKindError,
        "unsupported function_def kind {kind!r} in {manifest}; expected {allowed}",
    )
    # ``load_function_def_or_raise`` wrong kind -- streamer pod only
    # runs ``streamPublisherDef`` functions.

    RS_5 = ErrorDef(
        NoPluginError,
        "no stream-source plugin registered for {src_type}: {detail}",
    )
    # ``get_stream_src`` ``KeyError``.

    RS_6 = ErrorDef(
        PluginFailedError,
        "stream plugin run() failed: {detail}",
    )
    # Unhandled exception escaping ``plugin.run(ctx, conn, src)``.

    RS_7 = ErrorDef(
        UnsupportedStageSchemeError,
        "stream-publisher function_stage URI {uri} uses scheme {scheme!r} "
        "which is not yet supported by run-streamer "
        "(only file:// is supported today)",
    )
    # Cloud schemes (``s3://`` / ``az://`` / ``gcs://``) will land once
    # the streamer pod's deployment requires them.

    RS_8 = ErrorDef(
        WriteFailedError,
        "failed to write journal {path}: {detail}",
    )
    # Raised when ``Journal.save`` hits an ``OSError`` at end of run.
