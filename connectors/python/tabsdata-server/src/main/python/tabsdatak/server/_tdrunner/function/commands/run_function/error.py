#
# Copyright 2026 Tabs Data Inc.
#

"""Error model for the run-function command (exit codes 10..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    BadBundleError as _BadBundleError,
    BadInputParquetError as _BadInputParquetError,
    BadManifestError as _BadManifestError,
    BadRequestError as _BadRequestError,
    BadStateInError as _BadStateInError,
    BadStateOutError as _BadStateOutError,
    BadUserCodeError as _BadUserCodeError,
    CommandRuntimeException,
    NoPluginError as _NoPluginError,
)


class BadRequestError(_BadRequestError):
    exit_code = 10


class BadBundleError(_BadBundleError):
    """``function_def.json`` missing, unreadable, malformed, or its
    ``kind`` isn't one of publisherDef / transformerDef / subscriberDef.

    Bundle-shape failures and unknown-kind failures share this exit
    code to preserve the pre-refactor mapping (the test suite expects
    that)."""

    exit_code = 11


class BadUserCodeError(_BadUserCodeError):
    """User function couldn't be resolved (pydantic
    ``ValidationError``), or its slot-parameter annotations are
    unrecognised."""

    exit_code = 12


class BadManifestError(_BadManifestError):
    """Inputs manifest missing, unreadable, malformed, or its
    entry-count doesn't match the user function's slot count, or a
    SINGLE-mode slot has multiple files."""

    exit_code = 14


class BadInputParquetError(_BadInputParquetError):
    """An input parquet listed in the manifest can't be opened or
    schema-scanned."""

    exit_code = 15


class NoPluginError(_NoPluginError):
    """The subscriber's destination class has no registered DestDef."""

    exit_code = 16


class UserFunctionFailedError(CommandRuntimeException):
    """The user-decorated function raised."""

    exit_code = 17


class OutputShapeMismatchError(CommandRuntimeException):
    """User function's return didn't match the expected shape
    (wrong tuple length, wrong type, non-LazyFrame value)."""

    exit_code = 18


class OutputWriteFailedError(CommandRuntimeException):
    """A LazyFrame returned by the user function couldn't be sunk
    to its target parquet."""

    exit_code = 19


class OutputsManifestWriteFailedError(CommandRuntimeException):
    """The outputs manifest couldn't be written."""

    exit_code = 20


class BadStateInError(_BadStateInError):
    """``hydrate_ctx_from_state_or_raise`` failure -- read side."""

    exit_code = 21


class BadStateOutError(_BadStateOutError):
    """``drain_ctx_to_state_or_raise`` failure -- write side."""

    exit_code = 21


class RunFunctionErrorCode(ErrorCode):
    RF_1 = ErrorDef(
        BadRequestError,
        "unreadable request.yaml {path}: {detail}",
    )
    # ``load_request_or_raise``.

    RF_2 = ErrorDef(
        BadBundleError,
        "could not load function_def.json from {bundle_dir}: {detail}",
    )
    # ``load_function_def_or_raise`` bundle-shape failure.

    RF_3 = ErrorDef(
        BadUserCodeError,
        "could not resolve user function from {bundle_dir}: {detail}",
    )
    # ``load_function_def_or_raise`` user-code failure.

    RF_4 = ErrorDef(
        BadBundleError,
        "unsupported function_def kind {kind!r} in {manifest}; expected {allowed}",
    )
    # ``load_function_def_or_raise`` wrong-kind. Same exit code as RF_2
    # (both surface as "bad bundle") to preserve the pre-refactor
    # mapping.

    RF_5 = ErrorDef(
        BadManifestError,
        "could not load inputs manifest {path}: {detail}",
    )
    # ``load_inputs_manifest_or_raise``.

    RF_6 = ErrorDef(
        BadUserCodeError,
        "cannot resolve type hints for user function: {detail}",
    )
    # ``_infer_table_modes`` ``get_type_hints`` failure.

    RF_7 = ErrorDef(
        BadUserCodeError,
        (
            "parameter {param!r} of user function has an unrecognised "
            "slot annotation; expected TableFrameSpec or TableFramesSpec"
        ),
    )
    # ``_infer_table_modes`` unrecognised annotation. Same exit code
    # as RF_6.

    RF_8 = ErrorDef(
        NoPluginError,
        "no destination plugin registered for {dest_type}: {detail}",
    )
    # ``_output_table_names`` ``KeyError`` from ``get_dest``.

    RF_9 = ErrorDef(
        BadInputParquetError,
        "failed to scan input parquet {path}: {detail}",
    )
    # ``_scan_parquet_or_fail`` ``pl.scan_parquet`` failure.

    RF_10 = ErrorDef(
        BadManifestError,
        (
            "inputs manifest has {entries} entries but the user "
            "function declares {slots} input slots"
        ),
    )
    # main() entry/slot count mismatch.

    RF_11 = ErrorDef(
        BadManifestError,
        "SINGLE-mode slot has {count} files; expected 0 or 1",
    )
    # ``_materialise_input_slot`` SINGLE mode > 1 files. Same exit
    # code as RF_5 / RF_10 (all are manifest-shape failures).

    RF_12 = ErrorDef(
        UserFunctionFailedError,
        "user function raised: {detail}",
    )
    # ``_invoke_user_function`` raised inside the user code.

    RF_13 = ErrorDef(
        OutputShapeMismatchError,
        "user function must return a tuple, got {actual_type}",
    )
    # ``_invoke_user_function`` return-type mismatch.

    RF_14 = ErrorDef(
        OutputShapeMismatchError,
        (
            "user function returned {returned} value(s); output_tables "
            "declares {expected} ({tables})"
        ),
    )
    # ``_invoke_user_function`` tuple-length mismatch.

    RF_15 = ErrorDef(
        OutputShapeMismatchError,
        (
            "output {slot} ({table!r}) must be a polars.LazyFrame or "
            "None, got {actual_type}"
        ),
    )
    # ``_invoke_user_function`` per-entry type mismatch.

    RF_16 = ErrorDef(
        OutputWriteFailedError,
        "failed to write output {table!r} to {path}: {detail}",
    )
    # ``_sink_outputs`` ``sink_parquet`` failure.

    RF_17 = ErrorDef(
        OutputsManifestWriteFailedError,
        "failed to write outputs manifest {path}: {detail}",
    )
    # ``write_outputs_manifest`` ``OSError``.

    RF_18 = ErrorDef(
        BadStateInError,
        "unreadable state-in {state_file}: {detail}",
    )
    # ``hydrate_ctx_from_state_or_raise`` -- state parquet exists but
    # couldn't be decoded.

    RF_19 = ErrorDef(
        BadStateOutError,
        "unwritable state-out {state_file}: {detail}",
    )
    # ``drain_ctx_to_state_or_raise`` -- ctx scratch couldn't be
    # persisted.
