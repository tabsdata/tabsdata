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
        return enum_class


class ErrorCode(Enum, metaclass=_ErrorCodeMeta):
    def exception(
        self,
        cause: Exception | None = None,
        **kwargs: Any,
    ) -> "TabsdataException":
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
    def __init__(
        self,
        api_error: ErrorCode,
        message: str = "",
        cause: Exception | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
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
    # The value-reference family (`_ValOrRef` / `Env` / `Secret` and their
    # `LiteralEnv` / `LiteralSecret` literal siblings) maps to a fixed
    # cluster of codes, one per validation site.
    API_5 = ErrorDef(ApiInternalException, "Cannot create _ValOrRef")
    # `_ValOrRef` dataclass decorator (abstract base for Env / Secret)
    API_6 = ErrorDef(
        ApiRuntimeException,
        "Env var {name} value `{value!r}` does not match pattern `{pattern!r}`",
    )
    # `_ValOrRef._validate()` -- shared pattern-mismatch raise for both
    # `Env` / `Secret` subclasses with a `_value_pattern` ClassVar.
    API_7 = ErrorDef(ApiInitException, "Cannot create Env")
    # `Env` dataclass decorator
    API_8 = ErrorDef(ApiRuntimeException, "Env var {name} not defined")
    # `Env._value()` -- env var unset at resolve time
    API_9 = ErrorDef(ApiInitException, "Invalid inline env var value")
    # `LiteralEnv` dataclass decorator
    API_10 = ErrorDef(ApiInitException, "Cannot create Secret")
    # `Secret` dataclass decorator
    API_11 = ErrorDef(ApiInternalException, "Secret {name}, missing secret resolution ")
    # `Secret._value()` -- non-literal secret has no resolver in scope
    API_12 = ErrorDef(ApiInitException, "Invalid inline secret value")
    # `LiteralSecret` dataclass decorator

    # --- Connections (tabsdatak.api) --------------------------------
    API_13 = ErrorDef(ApiInitException, "Cannot create NamedConn")
    # NamedConn

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
