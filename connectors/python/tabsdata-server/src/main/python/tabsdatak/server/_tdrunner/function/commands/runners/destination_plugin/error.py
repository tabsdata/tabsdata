#
# Copyright 2026 Tabsdata Inc.
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
    WriteFailedError as _WriteFailedError,
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
    """Outputs manifest missing, unreadable, malformed, or contains an
    entry without a ``uri`` (subscribers must return concrete frames for
    every slot)."""

    exit_code = 16


class OutputCopyFailedError(_WriteFailedError):
    """A landed output could not be read from storage or copied to the
    local file handed to the plugin."""

    exit_code = 17


class RunDestinationPluginErrorCode(ErrorCode):
    RDP_1 = ErrorDef(
        BadRequestError,
        "unreadable request.yaml {path}: {detail}",
    )
    # ``Request.load``.

    RDP_2 = ErrorDef(
        BadBundleError,
        "could not load function_def.json from {bundle_folder}: {detail}",
    )
    # ``load_function_def_or_raise`` bundle-shape failure (manifest
    # missing, unreadable, malformed JSON, unexpected error).

    RDP_3 = ErrorDef(
        BadUserCodeError,
        "could not resolve user function from {bundle_folder}: {detail}",
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
    # ``_write_tables`` catch-all for plugin exceptions.

    RDP_7 = ErrorDef(
        BadManifestError,
        "could not load outputs manifest {path}: {detail}",
    )
    # ``OutputManifest.load`` ``ManifestError`` (missing / malformed).

    RDP_8 = ErrorDef(
        BadManifestError,
        (
            "output slot {slot} ({name!r}) is null — subscribers must return "
            "a concrete frame for every output slot"
        ),
    )
    # ``DestinationHandoff.seed`` ``MissingOutputError``: an entry without a ``uri``.

    RDP_9 = ErrorDef(
        OutputCopyFailedError,
        "failed to copy landed output {slot} from {path}: {detail}",
    )
    # ``DestinationHandoff.seed`` ``OutputCopyError``: the landing file could not be
    # read or copied.
