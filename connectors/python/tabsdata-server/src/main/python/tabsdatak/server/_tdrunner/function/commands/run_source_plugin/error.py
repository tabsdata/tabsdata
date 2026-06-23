#
# Copyright 2026 Tabs Data Inc.
#

"""Error model for the run-source-plugin command (exit codes 10..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    BadBundleError as _BadBundleError,
    BadRequestError as _BadRequestError,
    BadStateInError as _BadStateInError,
    BadStateOutError as _BadStateOutError,
    BadUserCodeError as _BadUserCodeError,
    CommandRuntimeException,
    ManifestWriteFailedError as _ManifestWriteFailedError,
    NoPluginError as _NoPluginError,
    PluginFailedError as _PluginFailedError,
    WrongKindError as _WrongKindError,
)


class BadRequestError(_BadRequestError):
    exit_code = 10


class BadBundleError(_BadBundleError):
    """``function_def.json`` missing, unreadable, or malformed."""

    exit_code = 11


class BadUserCodeError(_BadUserCodeError):
    exit_code = 12


class WrongKindError(_WrongKindError):
    """Bundle's ``kind`` isn't ``publisherDef``."""

    exit_code = 13


class NoPluginError(_NoPluginError):
    """No ``SrcPlugin`` is registered for the resolved source class."""

    exit_code = 14


class PluginFailedError(_PluginFailedError):
    """The source plugin's ``read_in()`` raised, or it produced the
    wrong number of slots."""

    exit_code = 15


class ManifestWriteFailedError(_ManifestWriteFailedError):
    """``--inputs-manifest`` could not be written."""

    exit_code = 16


class BadStateInError(_BadStateInError):
    """``hydrate_ctx_from_state_or_raise`` failure -- read side."""

    exit_code = 17


class BadStateOutError(_BadStateOutError):
    """``drain_ctx_to_state_or_raise`` failure -- write side."""

    exit_code = 17


class UnsupportedStageSchemeError(CommandRuntimeException):
    """``info.function_stage.uri`` uses a scheme the stream-publisher
    substitution does not yet support (today: only ``file://``).

    Only relevant on the stream-publisher path -- a regular
    ``@publisher`` uses the user-declared ``conn`` directly and never
    looks at ``function_stage``.
    """

    exit_code = 18


class RunSourcePluginErrorCode(ErrorCode):
    RSP_1 = ErrorDef(
        BadRequestError,
        "unreadable request.yaml {path}: {detail}",
    )
    # ``load_request_or_raise``.

    RSP_2 = ErrorDef(
        BadBundleError,
        "could not load function_def.json from {bundle_dir}: {detail}",
    )
    # ``load_function_def_or_raise`` bundle-shape failure.

    RSP_3 = ErrorDef(
        BadUserCodeError,
        "could not resolve user function from {bundle_dir}: {detail}",
    )
    # ``load_function_def_or_raise`` user-code failure.

    RSP_4 = ErrorDef(
        WrongKindError,
        "unsupported function_def kind {kind!r} in {manifest}; expected {allowed}",
    )
    # ``load_function_def_or_raise`` wrong kind.

    RSP_5 = ErrorDef(
        NoPluginError,
        "no source plugin registered for {src_type}: {detail}",
    )
    # ``_resolve_plugin`` ``KeyError``.

    RSP_6 = ErrorDef(
        PluginFailedError,
        "plugin read_in failed: {detail}",
    )
    # ``_invoke_read_in`` catch-all.

    RSP_7 = ErrorDef(
        PluginFailedError,
        "plugin returned {returned} input slot(s); source cardinality is {expected}",
    )
    # ``_write_inputs_manifest`` cardinality mismatch. Same exit code
    # as RSP_6 (both are "the source plugin behaved unexpectedly").

    RSP_8 = ErrorDef(
        ManifestWriteFailedError,
        "failed to write inputs manifest {path}: {detail}",
    )
    # ``write_inputs_manifest`` ``OSError``.

    RSP_9 = ErrorDef(
        BadStateInError,
        "unreadable state-in {state_file}: {detail}",
    )
    # ``hydrate_ctx_from_state_or_raise`` -- previous run's state
    # parquet exists but couldn't be decoded.

    RSP_10 = ErrorDef(
        BadStateOutError,
        "unwritable state-out {state_file}: {detail}",
    )
    # ``drain_ctx_to_state_or_raise`` -- ctx scratch couldn't be
    # persisted (typically a bad value injected past the
    # ``store_attr`` validator).

    RSP_11 = ErrorDef(
        UnsupportedStageSchemeError,
        "stream-publisher function_stage URI {uri} uses scheme {scheme!r} "
        "which is not yet supported by run-source-plugin "
        "(only file:// is supported today)",
    )
    # Raised on the stream-publisher path when ``info.function_stage.uri``
    # is not a ``file://`` URI. Cloud schemes (``s3://`` / ``az://`` /
    # ``gcs://``) will land once the streamer pod runtime handles them.
