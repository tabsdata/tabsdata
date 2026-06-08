#
# Copyright 2026 Tabs Data Inc.
#

"""Error model for the run-destination-plugin command (exit codes 10..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    BadBundleError as _BadBundleError,
    BadManifestError as _BadManifestError,
    BadRequestError as _BadRequestError,
    BadUserCodeError as _BadUserCodeError,
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
    """Bundle's ``kind`` isn't ``subscriberDef``."""

    exit_code = 13


class NoPluginError(_NoPluginError):
    """No ``DestPlugin`` is registered for the resolved destination
    class."""

    exit_code = 14


class PluginFailedError(_PluginFailedError):
    """The destination plugin's ``write_out()`` raised an exception."""

    exit_code = 15


class BadManifestError(_BadManifestError):
    """Outputs manifest missing, unreadable, malformed, or contains a
    ``file: null`` entry (subscribers must return concrete frames for
    every slot)."""

    exit_code = 16


class RunDestinationPluginErrorCode(ErrorCode):
    RDP_1 = ErrorDef(
        BadRequestError,
        "unreadable request.yaml {path}: {detail}",
    )
    # ``load_request_or_raise``.

    RDP_2 = ErrorDef(
        BadBundleError,
        "could not load function_def.json from {bundle_dir}: {detail}",
    )
    # ``load_function_def_or_raise`` bundle-shape failure (manifest
    # missing, unreadable, malformed JSON, unexpected error).

    RDP_3 = ErrorDef(
        BadUserCodeError,
        "could not resolve user function from {bundle_dir}: {detail}",
    )
    # ``load_function_def_or_raise`` user-code failure (pydantic
    # ``ValidationError``).

    RDP_4 = ErrorDef(
        WrongKindError,
        "unsupported function_def kind {kind!r} in {manifest}; expected {allowed}",
    )
    # ``load_function_def_or_raise`` kind mismatch (bundle isn't a
    # subscriberDef).

    RDP_5 = ErrorDef(
        NoPluginError,
        "no destination plugin registered for {dest_type}: {detail}",
    )
    # ``_resolve_plugin`` ``KeyError`` from the connector registry.

    RDP_6 = ErrorDef(
        PluginFailedError,
        "plugin write_out failed: {detail}",
    )
    # ``_invoke_write_out`` catch-all for plugin exceptions.

    RDP_7 = ErrorDef(
        BadManifestError,
        "could not load outputs manifest {path}: {detail}",
    )
    # ``load_outputs_manifest_or_raise`` failure (missing / malformed).

    RDP_8 = ErrorDef(
        BadManifestError,
        (
            "output slot {slot} ({name!r}) is null — subscribers must return "
            "a concrete frame for every output slot"
        ),
    )
    # ``_tables_from_manifest`` rejects a ``file: null`` entry.
