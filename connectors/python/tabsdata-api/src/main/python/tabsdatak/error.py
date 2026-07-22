#
# Copyright 2026 Tabsdata Inc.
#

"""Error machinery for the tabsdata API.

Every framework error flows through three layers:

* :class:`ErrorCode` -- an :class:`enum.Enum` whose variants carry an
  :class:`ErrorDef` (exception class + message template). Concrete
  enums (e.g. :class:`ApiErrorCode`, ``MysqlErrorCode``) declare one
  variant per validation site.
* :class:`ErrorDef` -- ``(exception, message_template)`` tuple bound
  to each enum variant. The template is ``str.format_map``-applied
  with the kwargs passed to :meth:`ErrorCode.exception`.
* :class:`TabsdataException` -- base exception. Carries the
  triggering :class:`ErrorCode`, an optional cause, the kwargs used
  to render the message, and a captured user source frame (the
  filename + line where the user's code triggered the error -- on
  by default, gated by the ``TABSDATA_DEBUG_FRAMES`` env var).

The :class:`Api*Exception` family below partitions the API errors
into init-time / runtime / internal so handlers can react to the
right severity without matching on individual codes.
"""

from __future__ import annotations

import inspect
import os
from abc import ABCMeta
from dataclasses import dataclass
from enum import Enum, EnumMeta
from pathlib import Path
from typing import Any, Type

# Kill switch for the user-source-frame capture. The capture is
# **on by default**: the framework's exceptions are constructed at
# descriptor-build / decoration time -- declarative code that runs
# once per pipeline-load, not on a hot path -- so the per-exception
# stack-walk cost is negligible compared to the payoff of pinning
# every error to its user call site (especially for LLM iteration).
#
# Set this env var to a falsy value (``""``, ``"0"``, ``"false"``,
# ``"no"``, ``"off"`` -- case-insensitive) to disable. Any other
# value (including unset) keeps it on.
DEBUG_FRAMES_ENV_VAR = "TABSDATA_DEBUG_FRAMES"

_FALSY_ENV_VALUES = frozenset({"", "0", "false", "no", "off"})


def _debug_frames_enabled() -> bool:
    val = os.environ.get(DEBUG_FRAMES_ENV_VAR)
    if val is None:
        return True
    return val.strip().lower() not in _FALSY_ENV_VALUES


# Path components that mark a frame as "framework, not user code".
# A frame is skipped if any directory component of its filename
# matches one of these. ``tabsdatak`` covers the api package and
# every ``tabsdata-conn-*`` connector (they all live under a
# ``tabsdatak`` directory). ``pydantic`` covers the wrapping
# validator stack so we don't surface a frame inside pydantic
# instead of the user's call site.
_FRAMEWORK_PATH_PARTS = ("tabsdatak", "pydantic", "pydantic_core")


def _capture_user_source() -> str | None:
    try:
        # Walk the whole stack; the path filter -- not the index --
        # decides what's user code. Skip frame ``[0]`` (this
        # function) so we never report ourselves.
        for frame_info in inspect.stack()[1:]:
            filename = frame_info.filename
            if not filename or filename.startswith("<"):
                # Synthetic frames (``<frozen importlib._bootstrap>``,
                # ``<string>``, eval'd code) carry no useful source.
                continue
            parts = set(Path(filename).parts)
            if parts.intersection(_FRAMEWORK_PATH_PARTS):
                # Skip framework frames -- the user's call site is
                # somewhere up the stack from here.
                continue
            line = ""
            ctx = frame_info.code_context
            if ctx:
                line = ctx[0].strip()
            location = f"{filename}:{frame_info.lineno}"
            return f"{location}: {line}" if line else location
    except Exception:  # noqa: BLE001 -- defensive
        return None
    return None


# `eq=False` keeps default identity-based equality / hashing. Two
# ErrorDef instances with the same field values must not compare
# equal: Enum collapses members whose values are equal into aliases,
# which would silently merge distinct error codes.
@dataclass(frozen=True, eq=False)
class ErrorDef:
    """Static description of one error site: the exception class to
    raise plus a ``str.format_map`` message template.

    :param exception: :class:`TabsdataException` subclass to
        instantiate.
    :param message_template: Format string applied with the kwargs
        from :meth:`ErrorCode.exception` (missing keys are left as
        ``{name}`` placeholders).
    """

    exception: Type["TabsdataException"]
    message_template: str


class _SafeFormatDict(dict):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


class _ErrorCodeMeta(EnumMeta, ABCMeta):
    def __new__(mcs, cls_name, bases, classdict, **kwargs):
        enum_class = super().__new__(mcs, cls_name, bases, classdict, **kwargs)
        # The abstract base ``ErrorCode`` has no members; only validate
        # concrete subclasses that actually declare variants.
        if enum_class.__members__:
            for member in enum_class:
                if not isinstance(member.value, ErrorDef):
                    raise TypeError(
                        f"{cls_name}.{member.name}: enum variant value "
                        f"must be an ErrorDef "
                        f"(got {type(member.value).__name__})"
                    )
                exc = member.value.exception
                if not (isinstance(exc, type) and issubclass(exc, TabsdataException)):
                    raise TypeError(
                        f"{cls_name}.{member.name}: ErrorDef.exception "
                        f"must be a TabsdataException subclass "
                        f"(got {exc!r})"
                    )
                exc._api_error = member
        return enum_class


class ErrorCode(Enum, metaclass=_ErrorCodeMeta):
    """Abstract base for connector error-code enums.

    Concrete subclasses declare variants whose values are
    :class:`ErrorDef` instances. Call :meth:`exception` to render a
    template and raise the bound exception::

        raise MysqlErrorCode.MYSQL_5.exception(
            cause=sqlalchemy_err, uri=conn.uri,
        )
    """

    def exception(
        self,
        cause: Exception | None = None,
        **kwargs: Any,
    ) -> "TabsdataException":
        """Build a :class:`TabsdataException` for this code.

        :param cause: Underlying exception to chain via
            ``__cause__`` (the framework also sets
            ``__suppress_context__`` so the traceback reads as
            ``raise ... from cause``).
        :param kwargs: Substituted into the template; surplus keys
            are stashed on the resulting exception's ``extra``.
        """
        error_def: ErrorDef = self.value
        message = error_def.message_template.format_map(_SafeFormatDict(kwargs))
        return error_def.exception(
            api_error=self,
            message=message,
            cause=cause,
            extra=dict(kwargs),
        )

    def __str__(self) -> str:
        name = self.name
        pos = name.rfind("_")
        if pos > 0:
            name = name[:pos] + "-" + name[pos + 1 :]
        return name


class TabsdataException(Exception):
    """Base class for every framework error.

    Carries the originating :class:`ErrorCode` (``api_error``),
    optional underlying ``cause``, the kwargs used to render the
    message (``extra``), and -- when
    ``TABSDATA_DEBUG_FRAMES`` is enabled -- a ``filename:lineno:
    source`` snippet pinpointing the user's call site
    (``source``).
    """

    def __init__(
        self,
        api_error: ErrorCode | None = None,
        message: str = "",
        cause: Exception | None = None,
        extra: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        if api_error is None:
            api_error = getattr(type(self), "_api_error", None)
            if api_error is None:
                raise TypeError(
                    f"{type(self).__name__} is not bound to an ErrorCode; "
                    f"either pass api_error= explicitly or declare an "
                    f"ErrorDef(...) variant referencing it."
                )
            template = api_error.value.message_template
            message = template.format_map(_SafeFormatDict(kwargs))
            extra = dict(kwargs)
        self.api_error = api_error
        self.cause = cause
        self.extra = dict(extra or {})
        # User-source-frame capture is on by default (see the
        # `source` attribute docstring). Set TABSDATAK_DEBUG_FRAMES
        # to a falsy value to disable.
        self.source: str | None = (
            _capture_user_source() if _debug_frames_enabled() else None
        )
        parts: list[str] = [api_error.name]
        if message:
            parts.append(message)
        if cause is not None:
            parts.append(_render_cause(cause))
        rendered = ": ".join(parts)
        if self.source:
            rendered = f"{rendered} at {self.source}"
        super().__init__(rendered)
        # Mirror what ``raise ... from cause`` would do, so call
        # sites only need ``raise code.exception(cause=e)`` without
        # also writing ``from e`` to chain the traceback.
        if cause is not None:
            self.__cause__ = cause
            self.__suppress_context__ = True


def _render_cause(cause: Exception) -> str:
    errors_fn = getattr(cause, "errors", None)
    if not callable(errors_fn):
        return str(cause)
    try:
        errors = errors_fn()
    except (TypeError, ValueError, AttributeError):
        return str(cause)
    if not isinstance(errors, list) or not errors:
        return str(cause)
    parts: list[str] = []
    for err in errors:
        if not isinstance(err, dict):
            return str(cause)
        loc = ".".join(str(p) for p in err.get("loc", ()))
        msg = err.get("msg", "")
        parts.append(f"{loc}: {msg}" if loc else msg)
    return "; ".join(parts)


class ApiException(TabsdataException):
    """API Top level exception class"""

    pass


class ApiInitException(ApiException):
    """An API object could not be created because of attributes type/value issues"""

    pass


class ApiRuntimeException(ApiException):
    """An API object is not valid because of runtime conditions. I.e. an ENV not set"""

    pass


class ApiInternalException(ApiException):
    """An API internal error, a bug."""

    pass


class ApiErrorCode(ErrorCode):
    # --- Triggers (tabsdatak.api) -----------------------------------
    API_1 = ErrorDef(ApiInternalException, "Cannot create Trigger")
    # Trigger (abstract base, datetime-aware check)
    API_2 = ErrorDef(ApiInitException, "Cannot create CronTrigger")
    # CronTrigger
    API_3 = ErrorDef(ApiInitException, "Cannot create TablesTrigger")
    # TablesTrigger (fire on any of a named set of upstream tables)
    API_4 = ErrorDef(ApiInternalException, "Cannot create AllTablesTrigger")
    # AllTablesTrigger (fire on any upstream table change)

    # --- Value references (tabsdatak.api) ---------------------------
    # The `Secret` value reference maps to three codes. (API_7-10 / API_12
    # were retired with the `_ValOrRef` / `Var` / `LiteralEnv` /
    # `LiteralSecret` types; their slots are left vacant so the surviving
    # codes keep their stable numbers.)
    API_5 = ErrorDef(ApiInitException, "Secret value must be a string, got {type}")
    # `Secret.__init__` -- a non-string, non-None value was passed.
    API_6 = ErrorDef(
        ApiRuntimeException,
        "Secret {name} value `{value!r}` does not match pattern `{pattern!r}`",
    )
    # `SecretValueConstraints` -- field-site pattern-mismatch raise for a
    # `Secret` carrying a hydrated value that fails the field's regex.
    API_11 = ErrorDef(ApiInternalException, "Secret {name}, missing secret resolution ")
    # `Secret.value()` -- reading the value of a dehydrated secret

    # --- Connections --------------------------------------------------
    # (API_13 was retired with `NamedConn`; connections are now resolved
    # from the function's collection, not declared by name on the flow.
    # Its slot is left vacant so the surviving codes keep their numbers.)

    # --- Source / destination / action bases (tabsdatak.spi) --------
    # Tag for every concrete `Src` / `Dest` subclass, every `SrcDef` /
    # `streamSrcDef` / `DestDef`, and the conn-field type-checks those
    # bindings run on construction.
    API_14 = ErrorDef(ApiInitException, "Cannot create SrcDef")
    # SrcDef
    API_15 = ErrorDef(ApiInitException, "Cannot create streamSrcDef")
    # streamSrcDef
    API_16 = ErrorDef(ApiInitException, "Cannot create DestDef")
    # DestDef

    # --- Flow primitives (tabsdatak._info) --------------------------
    # The @publisher / @transformer / @subscriber / @stream_publisher
    # factories construct the matching Info dataclass directly; that
    # construction is the single validation surface for both
    # argument-type and signature-shape failures, so the factory has
    # no separate code.
    API_17 = ErrorDef(ApiInternalException, "@publisher validation failed")
    # PublisherInfo dataclass (covers @publisher factory args + sig)
    API_18 = ErrorDef(ApiInternalException, "@stream_publisher validation failed")
    # streamPublisherInfo dataclass (covers @stream_publisher factory args + sig)
    API_19 = ErrorDef(ApiInternalException, "@transformer validation failed")
    # TransformerInfo dataclass (covers @transformer factory args + sig)
    API_20 = ErrorDef(ApiInternalException, "@subscriber validation failed")
    # SubscriberInfo dataclass (covers @subscriber factory args + sig)
    API_21 = ErrorDef(ApiInitException, "Cannot create StageManifestEntry")
    # StageManifestEntry
    API_22 = ErrorDef(ApiInitException, "Cannot create TableFileInput")
    # TableFileInput
    API_23 = ErrorDef(ApiInitException, "Cannot create Entity")
    # Entity
