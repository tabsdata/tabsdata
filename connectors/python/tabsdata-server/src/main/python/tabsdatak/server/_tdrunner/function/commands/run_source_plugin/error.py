#
# Copyright 2026 Tabs Data Inc.
#

"""Error model for the run-source-plugin command (exit codes 10..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    CommandInitException,
    CommandRuntimeException,
)


class BadRequestError(CommandRuntimeException):
    """``--request`` file missing, unreadable, or malformed."""

    exit_code = 10


class BadBundleError(CommandRuntimeException):
    """``function_def.json`` missing, unreadable, or malformed."""

    exit_code = 11


class BadUserCodeError(CommandInitException):
    """User function couldn't be resolved from the bundle (pydantic
    ``ValidationError``)."""

    exit_code = 12


class WrongKindError(CommandRuntimeException):
    """Bundle's ``kind`` isn't ``publisherDef``."""

    exit_code = 13


class NoPluginError(CommandRuntimeException):
    """No ``SrcPlugin`` is registered for the resolved source class."""

    exit_code = 14


class PluginFailedError(CommandRuntimeException):
    """The source plugin's ``read_in()`` raised, or it produced the
    wrong number of slots."""

    exit_code = 15


class ManifestWriteFailedError(CommandRuntimeException):
    """``--inputs-manifest`` could not be written."""

    exit_code = 16


class BadInitialValuesError(CommandRuntimeException):
    """The on-disk state file (state-in or state-out) doesn't decode
    as a valid initial_values Parquet."""

    exit_code = 17


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
        BadInitialValuesError,
        "unreadable/unwritable state {state_file}: {detail}",
    )
    # ``hydrate_ctx_from_state_or_raise`` / ``drain_ctx_to_state_or_raise``.
