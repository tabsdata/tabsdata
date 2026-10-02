#
# Copyright 2026 Tabsdata Inc.
#

"""Error machinery for tdrunner commands.

Same three-layer pattern as :mod:`tabsdatak.error`:

* :class:`CommandException` -- base (a :class:`TabsdataException`),
  carries an ``exit_code`` class attribute that
  :func:`tabsdatak.server._tdrunner.function._lib.cli.run_command`
  maps to the process exit code.
* :class:`CommandInitException` / :class:`CommandRuntimeException` /
  :class:`CommandInternalException` -- severity tiers, mirroring the
  ``Api{Init,Runtime,Internal}Exception`` split.
* Per-site exception classes inheriting from the matching severity,
  each pinning its own ``exit_code``.
* :class:`CommandErrorCode` -- the enum binding ``ErrorDef(exception,
  template)`` per cross-command site. Per-command sites live in their
  own ``commands/<name>/error.py``.

Exit-code semantics (per ``docs/01-CommandBasics.md``):

* ``1``       -- invalid or missing options (:class:`InvalidOptionsError`).
* ``2``       -- undefined env in an ``env:<VAR>`` option (:class:`UndefinedEnvError`).
* ``3``       -- unhandled / unexpected exception (:class:`UnexpectedError`).
* ``4``       -- inconsistent state, e.g. ``CommandException`` raised with
  ``exit_code=0`` (:class:`InconsistentError`).
* ``5``       -- premature exit, ``SystemExit`` intercepted in command body
  (:class:`PrematureError`).
* ``6..9``    -- reserved.
* ``10..``    -- command-specific (see ``commands/<command>/error.py``).
"""

from __future__ import annotations

from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
    TabsdataException,
)

EXIT_INVALID_OPTIONS: int = 1
EXIT_UNDEFINED_ENV: int = 2
EXIT_UNEXPECTED: int = 3
EXIT_INCONSISTENT: int = 4
EXIT_PREMATURE: int = 5
EXIT_BAD_CONFIG: int = 6
COMMAND_SPECIFIC_MIN_EXIT_CODE: int = 10


# --- bases / severity ---------------------------------------------------


class CommandException(TabsdataException):
    """Base class for every tdrunner-command failure.

    Concrete leaf subclasses pin their own ``exit_code`` class attribute;
    :func:`run_command` reads it to return the process exit code.
    """

    exit_code: int  # set on each concrete leaf subclass


class CommandInitException(CommandException):
    """Construction-time precondition failed (bad options, malformed
    config, ...). Mirrors :class:`tabsdatak.error.ApiInitException`."""


class CommandRuntimeException(CommandException):
    """Runtime precondition failed (missing env var, missing file,
    unreachable endpoint, ...). Mirrors
    :class:`tabsdatak.error.ApiRuntimeException`."""


class CommandInternalException(CommandException):
    """Internal bug; should never reach the user. Mirrors
    :class:`tabsdatak.error.ApiInternalException`."""


# --- shared per-site exception classes ---------------------------------


class InvalidOptionsError(CommandInitException):
    """Invalid or missing command-line options (exit 1)."""

    exit_code = EXIT_INVALID_OPTIONS


class UndefinedEnvError(CommandRuntimeException):
    """``env:<VAR>`` reference whose environment variable is unset
    (exit 2)."""

    exit_code = EXIT_UNDEFINED_ENV


# --- orchestration-boundary exceptions ---------------------------------
#
# These are raised by the ``command`` decorator's outer try/except
# wrapper in :mod:`tabsdatak.server._tdrunner.function._lib.cli`. A
# command body should normally raise a per-command typed exception
# (its own ``<Cmd>ErrorCode`` enum); these three are the fallback
# classes for failures that escape that contract.


class UnexpectedError(CommandInternalException):
    """An unhandled, non-:class:`CommandException` exception escaped
    the command body. The decorator's outer ``except Exception`` arm
    wraps it and exits with code 3."""

    exit_code = EXIT_UNEXPECTED


class InconsistentError(CommandInternalException):
    """A :class:`CommandException` reached the orchestration layer
    with ``exit_code=0``. Treated as an internal-bug exit code so the
    pod reports a non-zero status to the responder."""

    exit_code = EXIT_INCONSISTENT


class PrematureError(CommandInternalException):
    """``SystemExit`` reached the orchestration layer (the command
    body called ``sys.exit`` or a library short-circuited the
    interpreter). The decorator promotes it to exit code 5 so the
    pod's failure-reporting path runs."""

    exit_code = EXIT_PREMATURE


# --- cross-command base classes ----------------------------------------
#
# Many command-side failures share their semantics across commands
# (e.g. "the --request file is missing or malformed" surfaces in 9 of
# the 11 commands). Declaring these classes here as bases lets each
# command's ``error.py`` subclass them to pin its own ``exit_code``
# while keeping the semantic type-identity shared. That lets
# cross-cutting code write ``except BadRequestError`` once and catch
# the conceptually-same failure from any command, instead of having
# to enumerate ``run_function.error.BadRequestError``,
# ``run_source_plugin.error.BadRequestError``, ... per command.
#
# Per-command leaves still pin their own ``exit_code`` class attribute;
# the bases here intentionally do NOT set one.


class BadRequestError(CommandRuntimeException):
    """``--request`` file missing, unreadable, or malformed."""


class BadBundleError(CommandRuntimeException):
    """Function bundle missing, unreadable, malformed, or the bundle's
    ``kind`` isn't one the command accepts."""


class BadUserCodeError(CommandInitException):
    """User function couldn't be resolved from the bundle (pydantic
    ``ValidationError`` -- typically because ``callable_from_str``
    couldn't import the user package)."""


class WrongKindError(CommandRuntimeException):
    """Bundle's ``kind`` value is well-formed but isn't accepted by
    this command (e.g. ``publisherDef`` to a subscriber-only
    command)."""


class BadManifestError(CommandRuntimeException):
    """A manifest file (inputs / outputs / state) is missing,
    unreadable, malformed, or its content disagrees with what the
    command expects (entry count, slot shape, ...)."""


class ManifestMismatchError(CommandRuntimeException):
    """Manifest is structurally valid but disagrees with the
    request -- e.g. a declared output table has no manifest entry."""


class ManifestWriteFailedError(CommandRuntimeException):
    """A manifest could not be written (missing parent dir,
    permission, ...)."""


class WriteFailedError(CommandRuntimeException):
    """Output file (non-manifest) could not be written."""


class BadEndpointSchemeError(CommandRuntimeException):
    """A request endpoint uses a URI scheme this command doesn't
    know how to fetch from / upload to."""


class BadStorageConfigError(CommandRuntimeException):
    """The staged storage config could not be read, or a cloud location
    references a mount absent from it."""

    exit_code = EXIT_BAD_CONFIG


class NoPluginError(CommandRuntimeException):
    """No plugin (Src / Dest / ...) is registered for the resolved
    class -- the connector package isn't installed in the runtime."""


class PluginFailedError(CommandRuntimeException):
    """A plugin call (``read_in`` / ``write_out`` / ...) raised, or
    the plugin returned a shape that violates its contract."""


class BadInputParquetError(CommandRuntimeException):
    """An input parquet listed in the manifest can't be opened or
    schema-scanned."""


class BadStateInError(CommandRuntimeException):
    """The on-disk ``state-in`` Parquet (previous run's per-flow
    scratch) couldn't be hydrated into the ctx. Read-side failure."""


class BadStateOutError(CommandRuntimeException):
    """The ctx's scratch couldn't be drained to its ``state-out``
    Parquet. Write-side failure."""


# --- enum binding the shared templates ---------------------------------


class CommandErrorCode(ErrorCode):
    """Cross-command error sites. Per-command sites live in
    ``commands/<name>/error.py``."""

    CMD_1 = ErrorDef(
        InvalidOptionsError,
        "invalid or missing options: {detail}",
    )
    # Raised by ``_commons.run.command.Parser.error`` and by ``resolve_flag`` when the
    # resolved literal isn't ``true`` / ``false``.

    CMD_2 = ErrorDef(
        UndefinedEnvError,
        "environment variable {var!r} referenced via env: but not defined",
    )
    # Raised by ``_lib.cli.resolve_value`` when an ``env:<VAR>`` reference
    # resolves to an unset variable.

    CMD_3 = ErrorDef(
        BadStorageConfigError,
        "The storage configuration at '{path}' could not be read: {detail}.",
    )
    # Raised by ``_lib.mounts.load_mount_option_keys`` on read / parse failure.

    CMD_4 = ErrorDef(
        BadStorageConfigError,
        "The cloud location '{uri}' references the mount '{mount}', which is "
        "not present in the storage configuration.",
    )
    # Raised by ``_lib.transporter2`` when a location's mount id has no entry
    # in the storage config.

    CMD_5 = ErrorDef(
        UnexpectedError,
        "The transporter mode '{mode}' is not supported.",
    )
    # Raised by ``_lib.transporter`` when TRANSPORTER_MODE is neither the
    # legacy nor the current implementation.
