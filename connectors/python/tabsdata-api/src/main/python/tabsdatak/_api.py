#
# Copyright 2026 Tabsdata Inc.
#

"""Internal framework primitives shared across :mod:`tabsdatak.api`
/ :mod:`tabsdatak.spi` / :mod:`tabsdatak._info`.

Most of the surface here is private. The pieces connector
packages import as ``tabsdatak._api`` are:

* :func:`dataclass` -- the framework dataclass decorator. Wraps
  :func:`pydantic.dataclasses.dataclass` and translates pydantic
  ``ValidationError`` into the
  :class:`~tabsdatak.error.TabsdataException` family with a
  caller-chosen :class:`~tabsdatak.error.ErrorCode`. Used by
  every concrete ``Conn`` / ``Src`` / ``Dest`` / ``StreamSrc`` /
  position dataclass.
* :func:`validate_call` -- the function counterpart of
  :func:`dataclass`. Wraps :func:`pydantic.validate_call` and
  translates a ``ValidationError`` raised while validating a call's
  arguments the same way, against a caller-chosen
  :class:`~tabsdatak.error.ErrorCode`.
* :class:`JsonSerde` -- mixin providing JSON envelope round-trip
  (``to_json`` / ``from_json``) with a ``__kind__`` discriminator
  for polymorphic loading.
* :class:`DescriptorBase` -- :class:`JsonSerde` plus the tagging
  the runtime relies on.
* ``*_VALIDATION`` constants -- a :class:`ValueConstraints` per
  domain field shape (table name, cron mask, ...).
* :func:`callable_to_str` / :func:`callable_from_str` /
  :func:`qualified_class` / :func:`resolve_class` --
  ``*Info``-side helpers for JSON round-trip of functions /
  class references.
"""

from __future__ import annotations

import contextlib
import dataclasses
import functools
import hashlib
import importlib
import importlib.machinery
import importlib.util
import inspect
import json
import logging
import os
import re
import sys
import typing
from datetime import datetime
from typing import Any, Callable, ClassVar, TypeVar, Union, dataclass_transform

from croniter import croniter
from pydantic import (
    Field,
    GetCoreSchemaHandler,
    PrivateAttr,
    TypeAdapter,
    ValidationError,
    model_validator,
    validate_call as pydantic_validate_call,
)
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic.errors import PydanticSchemaGenerationError
from pydantic_core import core_schema

# Error codes and exceptions live in `tabsdatak.api_error` so callers
# only depend on that module when they want to react to errors.
from tabsdatak.error import ApiErrorCode, ConstraintError, ErrorCode

_C = TypeVar("_C", bound=type)
_F = TypeVar("_F", bound=Callable[..., Any])

logger = logging.getLogger(__name__)

# The batches of a stream publisher are spaced at least this far apart. A
# test lowers the floor with a marked value, `unsafe_seconds`, which
# `decode_seconds` reads back; nothing else ever does.
BATCH_FLOOR_SECS = 60
SECONDS_BASE = 31_415_926_535


def decode_seconds(key: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"The value of {key} must be a whole number of seconds; it is {value!r}"
        )
    if SECONDS_BASE <= value < SECONDS_BASE + BATCH_FLOOR_SECS:
        seconds = value - SECONDS_BASE
        logger.warning(
            f"The value of {key} lowers the batch floor to {seconds} "
            "second(s); this is not for production use"
        )
        return seconds
    if value < BATCH_FLOOR_SECS:
        raise ValueError(
            f"The value of {key} must be at least {BATCH_FLOOR_SECS} seconds; "
            f"it is {value}"
        )
    if value >= SECONDS_BASE:
        raise ValueError(f"The value of {key} is out of range: {value}")
    return value


def unsafe_seconds(seconds: int) -> int:
    if not 0 <= seconds < BATCH_FLOOR_SECS:
        raise ValueError(
            f"The seconds to mark must be in [0, {BATCH_FLOOR_SECS}); "
            f"they are {seconds}"
        )
    return SECONDS_BASE + seconds


# PEP 681: tells type checkers and IDEs that this decorator produces a
# dataclass-like class -- i.e. the returned class has an `__init__`
# synthesized from its annotated attributes. Without it, tools see a bare
# class and flag "unexpected argument" on every call site. Pydantic's own
# @pydantic.dataclasses.dataclass carries the same marker internally, which
# is why direct use of it does not warn.
@dataclass_transform(
    frozen_default=True,
    field_specifiers=(dataclasses.field, Field, PrivateAttr),
)
def dataclass(
    api_error: ErrorCode,
    **dc_kwargs: Any,
) -> Callable[[_C], _C]:
    """Decorator wrapping :func:`pydantic.dataclasses.dataclass`.

    Translates pydantic ``ValidationError`` raised during
    construction into ``api_error.exception(cause=e)`` so every
    framework dataclass surfaces a stable :class:`ErrorCode`.
    ``dc_kwargs`` are forwarded to pydantic; ``frozen=True`` is the
    default (override with ``frozen=False`` if you genuinely need
    mutability).
    """
    # API dataclasses are frozen by default: once constructed, instances
    # cannot be mutated (attribute assignment raises FrozenInstanceError).
    # This protects invariants that were validated at construction time --
    # if a field could be reassigned afterwards, the ApiError tag on the
    # original validation would no longer guarantee the instance is valid.
    # `setdefault` (not a hard-coded True) keeps the escape hatch: a caller
    # that genuinely needs mutability can opt out with
    # `@dataclass(ApiError.X, frozen=False)`.
    dc_kwargs.setdefault("frozen", True)

    def wrap(cls: _C) -> _C:
        # `pydantic_dataclass` is overloaded in its stubs -- called with a
        # class it returns the decorated class, but type checkers pick the
        # "decorator factory" overload first. Ignoring here keeps the
        # runtime behaviour and the outer `Callable[[_C], _C]` contract.
        decorated = pydantic_dataclass(cls, **dc_kwargs)  # type: ignore[call-overload]
        original_init = decorated.__init__  # type: ignore[union-attr]

        @functools.wraps(original_init)
        def __init__(self: Any, *args: Any, **kwargs: Any) -> None:
            try:
                original_init(self, *args, **kwargs)  # type: ignore[misc]
            except ValidationError as e:
                raise api_error.exception(cause=e)

        decorated.__init__ = __init__  # type: ignore[union-attr,method-assign]
        # Recorded so `JsonSerde.from_dict` / `from_json` translate the same
        # way construction does -- otherwise deserializing a bad descriptor
        # leaks a raw pydantic `ValidationError` with no error code.
        decorated.__api_error__ = api_error  # type: ignore[attr-defined]
        return decorated  # type: ignore[return-value]

    return wrap


def _positional_param_names(func: Callable[..., Any]) -> dict[int, str]:
    try:
        params = inspect.signature(func).parameters.values()
    except (TypeError, ValueError):
        return {}
    kinds = (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    return {i: p.name for i, p in enumerate(params) if p.kind in kinds}


def _name_positional_locs(
    e: ValidationError,
    names: dict[int, str],
) -> ValidationError:
    """Rewrite the argument indices in ``e``'s error locations to the
    parameter names they bind to.

    ``validate_call`` locates an argument passed positionally by its
    index, so an untouched message reads ``0: not a valid table name``
    where the :func:`dataclass` path reads ``name: ...``. Indices that
    bind to no parameter (a surplus positional argument) are left alone:
    they have no name to report.
    """
    if not names:
        return e
    line_errors: list[Any] = []
    renamed = False
    for err in e.errors(include_url=False):
        loc = err["loc"]
        if loc and isinstance(loc[0], int) and loc[0] in names:
            loc = (names[loc[0]], *loc[1:])
            renamed = True
        entry = {"type": err["type"], "loc": loc, "input": err.get("input")}
        if "ctx" in err:
            entry["ctx"] = err["ctx"]
        line_errors.append(entry)
    if not renamed:
        return e
    try:
        return ValidationError.from_exception_data(e.title, line_errors)
    except (TypeError, ValueError):
        # Rebuilding is a message-quality nicety; a shape pydantic
        # declines to reconstruct must not mask the original failure.
        return e


def validate_call(
    api_error: ErrorCode,
    **vc_kwargs: Any,
) -> Callable[[_F], _F]:
    """Decorator wrapping :func:`pydantic.validate_call`.

    The function counterpart of :func:`dataclass`: pydantic checks the
    call's arguments against the annotations, and the resulting
    ``ValidationError`` is translated into ``api_error.exception(cause=e)``
    so a bad call surfaces the same stable :class:`ErrorCode` -- and the
    same rendered per-argument detail, ``ConstraintError`` codes included
    -- that a bad descriptor construction does. Use it on functions and
    methods that carry validation but have no dataclass to hang it on.

    ``vc_kwargs`` are forwarded to pydantic (``config``,
    ``validate_return``); parameters pydantic cannot build a schema for
    need ``config=ConfigDict(arbitrary_types_allowed=True)``, the same
    escape hatch :func:`dataclass` callers pass as ``config``.
    """

    def wrap(func: _F) -> _F:
        validated = pydantic_validate_call(func, **vc_kwargs)
        positional = _positional_param_names(func)

        @functools.wraps(validated)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return validated(*args, **kwargs)
            except ValidationError as e:
                raise api_error.exception(cause=_name_positional_locs(e, positional))

        wrapper.__api_error__ = api_error  # type: ignore[attr-defined]
        return wrapper  # type: ignore[return-value]

    return wrap


# ---------------------------------------------------------------------------
# Field-site value rules.
#
# ``ValueConstraints`` gates the *text* of a field whatever shape carries
# it -- a plain ``str``, a :class:`~tabsdatak.api.Secret`, or a
# ``StrOrSecretSpec`` union. It runs as an after-validator on the parsed
# value and dispatches on that value rather than on the annotation, so one
# spelling covers every shape and both union arms are gated by the same
# rules (no second pattern for the secret arm). A dehydrated secret
# (``":secret:"``) carries no text and is skipped; the rules are
# re-applied whenever the runtime rehydrates it, because the hydrated tree
# is re-validated through the field.
#
# Each ``Rule`` pairs a check with the ``ErrorCode`` raised when it fails,
# so a rejection reports a domain message ("The value is not a valid S3
# bucket name") instead of the regex that did the rejecting. A field may
# carry several rules, each with its own code, so multi-part shapes stay
# diagnosable.
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Rule:
    """One field-value check paired with the error code it raises.

    :categories: error

    Build rules with the factories below rather than the constructor;
    each returns a ``Rule`` ready to hand to :class:`ValueConstraints`.

    Attributes:
        code: The `ErrorCode` raised when ``test`` fails.
        test: Predicate on the field's text -- ``True`` means valid.
        detail: Maps the text to the kwargs used to render ``code``'s
            message template.
        pattern: The regex, when this rule is a regex match -- see
            :attr:`ValueConstraints.pattern`.
    """

    code: ErrorCode
    test: Callable[[str], bool]
    detail: Callable[[str], dict[str, Any]] = lambda value: {"value": value}
    pattern: str | None = None

    def apply(self, text: str) -> None:
        """Raise :class:`~tabsdatak.error.ConstraintError` if ``text``
        fails this rule."""
        if not self.test(text):
            raise ConstraintError(self.code, **self.detail(text))

    @staticmethod
    def matches(pattern: str, code: ErrorCode) -> "Rule":
        """The text must fully match ``pattern``.

        Matching is always ``re.fullmatch``, so a leading ``^`` / trailing
        ``$`` in ``pattern`` is redundant.
        """
        compiled = re.compile(pattern)
        return Rule(
            code,
            lambda text: compiled.fullmatch(text) is not None,
            pattern=pattern,
        )

    @staticmethod
    def length(
        code: ErrorCode, *, min_len: int = 1, max_len: int | None = None
    ) -> "Rule":
        """The text length must fall within ``min_len``..``max_len``."""
        return Rule(
            code,
            lambda text: (
                min_len <= len(text) <= (max_len if max_len is not None else len(text))
            ),
            lambda text: {"length": len(text), "value": text},
        )

    @staticmethod
    def forbids(chars: str, code: ErrorCode) -> "Rule":
        """The text must contain none of ``chars``."""
        forbidden = frozenset(chars)
        return Rule(code, lambda text: not (forbidden & set(text)))

    @staticmethod
    def parses_as(
        type_: type,
        code: ErrorCode,
        *,
        ge: Any = None,
        le: Any = None,
        canonical: bool = False,
    ) -> "Rule":
        """The text must parse as ``type_`` (optionally within ``ge``..``le``).

        Parsing goes through the same pydantic coercion that
        ``tabsdatak.conn.common.types._convert`` applies at read time, so
        a field that validates here is guaranteed to convert there --
        one source of truth instead of a regex that restates the parser.

        Args:
            type_: Target type (e.g. ``int`` for a port, ``bool`` for a
                textual flag).
            code: Raised when the text does not parse or falls out of
                bounds.
            ge: Optional inclusive lower bound on the parsed value.
            le: Optional inclusive upper bound on the parsed value.
            canonical: When true, additionally require the text to be the
                canonical spelling of the parsed value -- rejecting the
                coercions pydantic would otherwise accept (``"0080"``,
                ``" 80"``, ``"+80"``, ``"8.0"`` for an ``int``). Leave
                false where several spellings are legitimately equivalent
                (``bool``: ``"yes"`` / ``"on"`` / ``"1"``).
        """
        annotation: Any = type_
        if ge is not None or le is not None:
            annotation = typing.Annotated[type_, Field(ge=ge, le=le)]
        adapter = TypeAdapter(annotation)

        def test(text: str) -> bool:
            try:
                parsed = adapter.validate_python(text)
            except ValidationError:
                return False
            return str(parsed) == text if canonical else True

        return Rule(code, test)

    @staticmethod
    def check(test: Callable[[str], bool], code: ErrorCode) -> "Rule":
        """Escape hatch for a rule no factory above expresses -- an FQN
        parse, a cron-semantics check, anything needing real code."""
        return Rule(code, test)


class ValueConstraints:
    """Field-site rules for a ``str``, a `Secret`, or a str-or-secret union.

    :categories: error

    Applied as ``Annotated[<shape>, ValueConstraints(<rule>, ...)]`` where
    ``<shape>`` is ``str``, `Secret`, or
    :data:`~tabsdatak.api.StrOrSecretSpec`. Rules run in order against the
    field's text and the first failure raises its own error code; a
    dehydrated secret carries no text and is skipped.

    Args:
        rules: The rules the value must satisfy, in evaluation order.
    """

    def __init__(self, *rules: Rule):
        if not rules:
            raise TypeError("ValueConstraints requires at least one Rule")
        for rule in rules:
            if not isinstance(rule, Rule):
                raise TypeError(
                    f"ValueConstraints expects Rule instances, got {type(rule)!r}"
                )
        self._rules = rules

    def __repr__(self) -> str:
        codes = ", ".join(str(rule.code) for rule in self._rules)
        return f"ValueConstraints({codes})"

    def validate(self, text: str) -> str:
        """Apply every rule to ``text``, returning it unchanged when all pass.

        The same entry point the field validator uses, exposed for callers
        that hold a value outside a descriptor field (and for tests).

        Raises:
            ConstraintError: the first rule that ``text`` fails.
        """
        for rule in self._rules:
            rule.apply(text)
        return text

    @property
    def pattern(self) -> str | None:
        """The first regex among these rules, or ``None`` if none is
        regex-shaped.

        Read by callers that want to pre-validate input against the field's
        shape before handing it over -- the `tdk` connection-template
        wizard does this to reject a bad entry at the prompt. It is a hint,
        not the contract: the rules themselves are the contract, and they
        run in full on construction.
        """
        return next(
            (rule.pattern for rule in self._rules if rule.pattern is not None), None
        )

    def __get_pydantic_core_schema__(
        self,
        source_type: Any,
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        rules = self._rules
        base = handler(source_type)

        def check(value: Any) -> Any:
            # `_Secret`, not the public `tabsdatak.api.Secret`: the public
            # class is a body-less subclass, so this covers both, and
            # staying inside `_api` keeps `api.py`'s own constrained fields
            # from importing a half-initialized `tabsdatak.api` while their
            # schemas are being built.
            text = value._value if isinstance(value, _Secret) else value
            # A dehydrated secret (``None``) and any non-text shape carry
            # nothing to check.
            if not isinstance(text, str):
                return value
            for rule in rules:
                rule.apply(text)
            return value

        return core_schema.no_info_after_validator_function(check, base)


# ---------------------------------------------------------------------------
# Field-level validators exposed for use in `tabsdatak.api` type aliases.
#
# Each is exposed as an `AfterValidator` constant so consumers can write:
#     SomeSpec: TypeAlias = Annotated[str, CRON_MASK_VALIDATION]
# without re-importing the underlying regex / function machinery.
# ---------------------------------------------------------------------------


# --- Identifier (table name): 1-64 chars, alphanumeric + underscore,
# --- must start with a letter.
_IDENTIFIER_RE_STR = r"^[A-Za-z][A-Za-z0-9_]{0,63}$"
_IDENTIFIER_RE = re.compile(_IDENTIFIER_RE_STR)

# --- Collection-qualified table name: <identifier> or
# --- <identifier>/<identifier>, no whitespace.
_COLL_IDENTIFIER_RE_STR = (
    r"^[A-Za-z][A-Za-z0-9_]{0,63}(?:/[A-Za-z][A-Za-z0-9_]{0,63})?$"
)
_COLL_IDENTIFIER_RE = re.compile(_COLL_IDENTIFIER_RE_STR)

# --- Version specs: a TableVersionSpec is a single point in a table's
# --- version history (VERSION_PATTERN); a TableVersionRangeSpec is a
# --- <version>..<version> range or the special range NEW, and the
# --- versions suffix also admits a comma-separated list
# --- (VERSIONS_PATTERN). Constant names, definitions and structure
# --- mirror server/libraries/td-objects/src/parse.rs (captures
# --- dropped); keep both sides in sync.
VERSION_PATTERN = (
    "^("
    "HEAD(\\^{0,10})"
    "|HEAD~([0-9]{1,7})"
    "|INITIAL(\\^{0,10})"
    "|INITIAL~([0-9]{1,7})"
    "|(([A-Z0-9]{26}|[0-9]{8}[A-Z]{1,3}[0-9]{1,10}))"
    ")$"
)
VERSION_REGEX = re.compile(VERSION_PATTERN)

UNNAMED_VERSION_PATTERN = (
    "(HEAD(\\^{0,10})|HEAD(~[0-9]{1,7})"
    "|INITIAL(\\^{0,10})|INITIAL(\\~[0-9]{1,7})|[A-Z0-9]{26})"
)
NEW_VERSION_PATTERN = "NEW"
VERSIONS_PATTERN = (
    "(" + NEW_VERSION_PATTERN + ")|"
    "^((" + UNNAMED_VERSION_PATTERN + ")"
    "|((" + UNNAMED_VERSION_PATTERN + "(," + UNNAMED_VERSION_PATTERN + ")+))"
    "|((" + UNNAMED_VERSION_PATTERN + ".." + UNNAMED_VERSION_PATTERN + ")))$"
)
VERSIONS_REGEX = re.compile(VERSIONS_PATTERN)

VERSIONS_MARKER_REGEX = "[^/@]+"
VERSIONED_TABLE_PATTERN = (
    "^((?P<collection>[^/]+)/)?(?P<table>[^@]+)(@(?P<versions>"
    + VERSIONS_MARKER_REGEX
    + "))?$"
)
VERSIONED_TABLE_REGEX = re.compile(VERSIONED_TABLE_PATTERN)

# --- Kubernetes CronJob schedule: 5-field standard Unix cron + the descriptors
# --- K8s documents. Quartz/Jenkins extensions and @reboot/@every are excluded.
#
# Two-layer validation:
#   1. Regex enforces *syntax* -- 5 whitespace-separated fields, each made of
#      `*`, digit-runs, or 3-letter month/DOW names, with `-` ranges, `/`
#      steps, and `,` lists. The {3} on names rejects Quartz tokens (L, W, H)
#      and composites (5L, 15W, 5#3) by shape, before croniter sees them.
#   2. croniter enforces *semantics* -- per-field ranges (minute 0-59,
#      hour 0-23, dom 1-31, month 1-12 or named, dow 0-7 or named) and
#      validates name spellings (JAN..DEC, SUN..SAT).
#
# Descriptors are accepted by exact (case-sensitive) string match because
# both layers above would otherwise reject them: the regex requires 5 fields,
# and croniter accepts non-K8s descriptors like @reboot / @every.
_K8S_CRON_DESCRIPTORS = frozenset({
    "@yearly",
    "@annually",
    "@monthly",
    "@weekly",
    "@daily",
    "@midnight",
    "@hourly",
})

_CRON_TOKEN_RE_STR = r"(?:\d+|[A-Za-z]{3})"
_CRON_ATOM_RE_STR = rf"(?:\*|{_CRON_TOKEN_RE_STR}(?:-{_CRON_TOKEN_RE_STR})?)"
_CRON_FIELD_RE_STR = rf"{_CRON_ATOM_RE_STR}(?:/\d+)?(?:,{_CRON_ATOM_RE_STR}(?:/\d+)?)*"
_UNIX_CRON_RE = re.compile(rf"^{_CRON_FIELD_RE_STR}(?:\s+{_CRON_FIELD_RE_STR}){{4}}$")


def _is_coll_table_versions_name(value: str) -> bool:
    match = VERSIONED_TABLE_REGEX.match(value)
    if not match:
        return False
    collection = match.group("collection")
    table = match.group("table")
    versions = match.group("versions")
    if collection is not None and not _IDENTIFIER_RE.fullmatch(collection):
        return False
    if not _IDENTIFIER_RE.fullmatch(table):
        return False
    if versions is not None:
        versions_match = VERSIONS_REGEX.search(versions)
        if not versions_match:
            return False
        # Mirror parse_versions' second phase: unless the NEW branch
        # (group 1) matched, every member must parse as a version.
        if versions_match.group(1) is None:
            if ".." in versions:
                members = versions.split("..")
            elif "," in versions:
                members = versions.split(",")
            else:
                members = [versions]
            for member in members:
                if not VERSION_REGEX.match(member):
                    return False
    return True


def _is_cron_syntax(value: str) -> bool:
    return value in _K8S_CRON_DESCRIPTORS or bool(_UNIX_CRON_RE.fullmatch(value))


def _is_cron_semantics(value: str) -> bool:
    # A descriptor never reaches croniter (it rejects some of the ones K8s
    # documents); the syntax rule already accepted it.
    return value in _K8S_CRON_DESCRIPTORS or croniter.is_valid(value)


TABLE_NAME_VALIDATION = ValueConstraints(
    Rule.matches(_IDENTIFIER_RE_STR, ApiErrorCode.API_24)
)
COLL_TABLE_NAME_VALIDATION = ValueConstraints(
    Rule.matches(_COLL_IDENTIFIER_RE_STR, ApiErrorCode.API_25)
)
COLL_TABLE_VERSIONS_NAME_VALIDATION = ValueConstraints(
    Rule.check(_is_coll_table_versions_name, ApiErrorCode.API_26)
)
CRON_MASK_VALIDATION = ValueConstraints(
    Rule.check(_is_cron_syntax, ApiErrorCode.API_27),
    Rule.check(_is_cron_semantics, ApiErrorCode.API_28),
)

NON_EMPTY_LIST_FIELD = Field(min_length=1)


# ---------------------------------------------------------------------------
# Secret -- implementation of the public `tabsdatak.api.Secret`.
#
# All behaviour lives here; the public class is a thin, body-less subclass so
# that (a) the public name stays `tabsdatak.api.Secret` and (b) every method
# uses `cls` / `type(self)`, so a `Secret`-typed field validates to `Secret`
# instances -- not `_Secret` -- without any code in `api.py`.
# ---------------------------------------------------------------------------


class _Secret:
    # Serialized (always dehydrated) marker.
    _DEHYDRATED = ":secret:"
    # Hydrated wire prefix accepted on deserialization ("secret:<value>").
    _PREFIX = "secret:"

    def __init__(self, value: str | None = None):
        # ``Secret("x")`` -> hydrated; ``Secret()`` -> dehydrated.
        if value is not None and not isinstance(value, str):
            raise ApiErrorCode.API_5.exception(type=type(value).__name__)
        self._value = value

    def value(self) -> str:
        """The hydrated value; raises if this secret is dehydrated."""
        if self._value is None:
            raise ApiErrorCode.API_11.exception(name=type(self).__name__)
        return self._value

    # --- serde helpers (a Secret round-trips to the bare ``":secret:"``
    # string, never an envelope) -----------------------------------------

    def to_dict(self) -> str:
        return TypeAdapter(type(self)).dump_python(self, mode="json")

    def to_json(self, indent: int | None = None) -> str:
        return TypeAdapter(type(self)).dump_json(self, indent=indent).decode()

    @classmethod
    def from_dict(cls, d: Any) -> "_Secret":
        # from_dict is deserialization -> decode via json-mode so the wire
        # forms (``":secret:"`` / ``"secret:<value>"``) are interpreted.
        return TypeAdapter(cls).validate_json(json.dumps(d))

    @classmethod
    def from_json(cls, s: str | bytes) -> "_Secret":
        return TypeAdapter(cls).validate_json(s)

    @classmethod
    def _parse(cls, s: str) -> str | None:
        # ``":secret:"`` -> dehydrated (None); ``"secret:<value>"`` ->
        # hydrated with ``<value>`` (which may be empty). Any other string
        # is rejected: an unprefixed value is not accepted on the wire.
        if s == cls._DEHYDRATED:
            return None
        if s.startswith(cls._PREFIX):
            return s[len(cls._PREFIX) :]
        raise ValueError(
            f"{cls.__name__} expects '{cls._PREFIX}<value>' or "
            f"'{cls._DEHYDRATED}', got {s!r}"
        )

    def __eq__(self, other: Any) -> bool:
        return type(self) is type(other) and self._value == other._value

    def __hash__(self) -> int:
        return hash((type(self), self._value))

    def __repr__(self) -> str:
        # Never leak the value through repr / logs.
        return f"{type(self).__name__}(***redacted***)"

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        def py_validate(value: Any) -> Any:
            # Python: the value is literal -- a Secret passes through, a str is
            # the literal secret value, None is dehydrated.
            if isinstance(value, cls):
                return value
            if value is None:
                return cls()
            if isinstance(value, str):
                return cls(value)
            raise ValueError(
                f"{cls.__name__} expects a string, None, or {cls.__name__} "
                f"instance (got {type(value).__name__})"
            )

        def json_validate(value: Any) -> Any:
            # JSON/wire: only the marked string forms; an unmarked string is
            # rejected by ``_parse``.
            if isinstance(value, str):
                return cls(cls._parse(value))
            raise ValueError(
                f"{cls.__name__} expects a string (got {type(value).__name__})"
            )

        def serialize(instance: Any) -> str:
            # A secret always serializes dehydrated -- its value never
            # lands in the serialized form.
            return cls._DEHYDRATED

        return core_schema.json_or_python_schema(
            json_schema=core_schema.no_info_plain_validator_function(json_validate),
            python_schema=core_schema.no_info_plain_validator_function(py_validate),
            serialization=core_schema.plain_serializer_function_ser_schema(
                serialize, return_schema=core_schema.str_schema()
            ),
        )


# ---------------------------------------------------------------------------
# str-or-secret union -- backs `tabsdatak.api.StrOrSecretSpec`.
# ---------------------------------------------------------------------------

# Wire prefix for the plain-string arm. On the wire the plain and secret arms
# are both strings, so a plain value is encoded ``"str:<value>"`` to keep it
# distinct from a secret (``"secret:<value>"`` / ``":secret:"``). The prefix is
# a *wire encoding* only: in Python the value is the literal string.
_STR_PREFIX = "str:"


class StrOrSecretHandler:
    """Annotation handler backing :data:`~tabsdatak.api.StrOrSecretSpec`.

    Splits validation by source (``json_or_python_schema``):

    * **python** -- the value is taken literally: a ``str`` is a plain value
      stored verbatim (no marker interpretation), a :class:`~tabsdatak.api.Secret`
      instance is a secret.
    * **json** -- the wire form is decoded: ``"str:<v>"`` -> plain ``<v>``,
      ``"secret:<v>"`` -> ``Secret(<v>)``, ``":secret:"`` -> dehydrated
      ``Secret``, and a bare ``"<v>"`` -> plain ``<v>``.

    Serialization dispatches on the runtime type: a plain ``str`` renders as
    ``"str:<value>"`` and a ``Secret`` as ``":secret:"``. So ``str:`` /
    ``:secret:`` are purely a wire encoding -- added on serialize, stripped on
    (json) deserialize, never seen in Python.
    """

    def __get_pydantic_core_schema__(
        self, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        from tabsdatak.api import Secret

        python_schema = core_schema.union_schema([
            core_schema.is_instance_schema(Secret),
            core_schema.str_schema(),
        ])

        def decode(value: Any) -> Any:
            if not isinstance(value, str):
                raise ValueError(f"expected a string, got {type(value).__name__}")
            if value == Secret._DEHYDRATED:
                return Secret()
            if value.startswith(Secret._PREFIX):
                return Secret(value[len(Secret._PREFIX) :])
            if value.startswith(_STR_PREFIX):
                return value[len(_STR_PREFIX) :]
            return value

        def serialize(value: Any) -> str:
            if isinstance(value, Secret):
                return Secret._DEHYDRATED
            return f"{_STR_PREFIX}{value}"

        return core_schema.json_or_python_schema(
            json_schema=core_schema.no_info_plain_validator_function(decode),
            python_schema=python_schema,
            serialization=core_schema.plain_serializer_function_ser_schema(
                serialize, return_schema=core_schema.str_schema()
            ),
        )


# ---------------------------------------------------------------------------
# Cross-cutting field-value check: every `datetime` field must be timezone-
# aware. Run as a `@model_validator(mode="after")` from the descriptor
# bases (`Src`, `Dest`, `Conn`, `Trigger`) so all tabsdata subclasses
# inherit it.
#
# Pydantic's default `str -> datetime` coercion accepts naive ISO strings
# (e.g. "2024-01-15T10:00:00"), and a manually-constructed
# `datetime(2024, 1, 15)` is also naive. Both leak into runtime as
# ambiguous wall-clock values; we reject them at the API boundary so the
# whole framework treats time as unambiguous from construction onward.
# ---------------------------------------------------------------------------


def check_tz_aware_datetime(obj: Any) -> Any:
    cls = type(obj)
    for fname in getattr(cls, "__pydantic_fields__", {}):
        v = getattr(obj, fname, None)
        if isinstance(v, datetime) and v.tzinfo is None:
            raise ValueError(
                f"{cls.__qualname__}.{fname}: datetime must be "
                f"timezone-aware (got naive {v.isoformat()})"
            )
    return obj


# ---------------------------------------------------------------------------
# Callable-signature validation for the @publisher / @stream_publisher /
# @transformer / @subscriber decorators. The contract is flat:
#
#   - The first ``input_count`` positional parameters are typed as a
#     "slot" -- one of the canonical aliases
#     :data:`tabsdatak.api.TableFrameSpec` (single-mode) or
#     :data:`tabsdatak.api.TableFramesSpec` (array-mode). Both aliases
#     are ``None``-bearing (see the docstring on them in ``api.py``);
#     bare ``TableFrame`` / ``list[TableFrame]`` is rejected because
#     it does not express the runtime's may-be-``None`` contract.
#   - An optional final positional parameter may be annotated as
#     ``TrxCtx`` (or a subclass). When present the framework passes a
#     transaction-context instance at call time; the function uses
#     ``ctx.get_attr`` / ``ctx.set_attr`` to read state from the
#     previous transaction and stage state for the next one. The
#     return shape is unaffected -- ctx flows out via in-place updates
#     on the context object, never via the return tuple.
#   - The return is annotated as a fixed-length
#     ``tuple[T1, ..., Tm]`` where ``m == output_count`` and each
#     ``Ti`` is a ``TableFrameSpec`` slot.
#
# The check catches the common mistakes (wrong slot count, missing
# return annotation, variable-length tuple, list-mode in a SINGLE
# source, missing ``| None``, ...) and emits a copy-pasteable
# "Expected signature" block at every raise site so an LLM iterating
# against the error knows what to write.
# ---------------------------------------------------------------------------


def _render_type(t: Any) -> str:
    # Defer the import to avoid an api -> _api -> api cycle on module
    # load.
    from tabsdatak.tableframe import TableFrame

    if t is type(None):
        return "None"
    if t is TableFrame:
        return "TableFrame"
    origin = typing.get_origin(t)
    if origin is None:
        return getattr(t, "__name__", repr(t))
    if origin in (Union, getattr(__import__("types"), "UnionType", None)):
        return " | ".join(_render_type(a) for a in typing.get_args(t))
    if origin is list:
        (inner,) = typing.get_args(t)
        return f"list[{_render_type(inner)}]"
    if origin is tuple:
        return "tuple[" + ", ".join(_render_type(a) for a in typing.get_args(t)) + "]"
    return repr(t)


# --- Slot-type predicates -------------------------------------------------
#
# A "slot type" is the type annotation of a single input parameter or
# of a single element of the return tuple. Two accepted shapes -- both
# carry an explicit ``| None`` because the runtime may pass ``None`` at
# the slot:
#
#   single mode (``TableFrameSpec``):
#     - ``TableFrame | None``
#
#   array mode (``TableFramesSpec``):
#     - ``list[TableFrame | None] | None``
#       (the list itself may be ``None``; individual list elements may
#       also be ``None``)


def _union_args(t: Any) -> tuple | None:
    """Return the args of a Union/UnionType, or None if ``t`` is not a
    union."""
    origin = typing.get_origin(t)
    union_origin = (Union, getattr(__import__("types"), "UnionType", None))
    if origin not in union_origin:
        return None
    return typing.get_args(t)


def _is_table_frame_or_none(t: Any) -> bool:
    # Matches ``TableFrame | None`` -- the canonical
    # :data:`tabsdatak.api.TableFrameSpec`.
    args = _union_args(t)
    if args is None:
        return False
    from tabsdatak.tableframe import TableFrame

    return set(args) == {TableFrame, type(None)}


def _is_single_slot(t: Any) -> bool:
    # Single-mode slot == ``TableFrameSpec`` == ``TableFrame | None``.
    return _is_table_frame_or_none(t)


def _is_array_slot(t: Any) -> bool:
    # Array-mode slot == ``TableFramesSpec`` ==
    # ``list[TableFrame | None] | None``. Both layers must carry
    # ``None`` for the slot to be considered well-typed.
    args = _union_args(t)
    if args is None:
        return False
    non_none = [a for a in args if a is not type(None)]
    if len(non_none) != 1 or type(None) not in args:
        return False
    inner_list = non_none[0]
    if typing.get_origin(inner_list) is not list:
        return False
    (element_t,) = typing.get_args(inner_list)
    return _is_table_frame_or_none(element_t)


def _matches_slot_mode(t: Any, mode: str) -> bool:
    if mode == "single":
        return _is_single_slot(t)
    if mode == "array":
        return _is_array_slot(t)
    if mode == "any":
        return _is_single_slot(t) or _is_array_slot(t)
    raise ValueError(f"unknown input_mode: {mode!r}")


def _slot_mode_hint(mode: str) -> str:
    if mode == "single":
        return "TableFrameSpec (TableFrame | None)"
    if mode == "array":
        return "TableFramesSpec (list[TableFrame | None] | None)"
    if mode == "any":
        return (
            "TableFrameSpec (TableFrame | None) or "
            "TableFramesSpec (list[TableFrame | None] | None)"
        )
    raise ValueError(f"unknown input_mode: {mode!r}")


def _format_expected_signature(
    name: str,
    *,
    input_count: int,
    input_mode: str,
    output_count: int,
    allow_trx_ctx: bool = True,
) -> str:
    if input_mode == "array":
        in_t = "TableFramesSpec"
    else:
        in_t = "TableFrameSpec"

    params = ",\n        ".join(f"in_{i + 1}: {in_t}" for i in range(input_count))
    if not params:
        params = "# no input parameters"
    out_args = ", ".join(["TableFrameSpec"] * output_count) if output_count > 0 else ""
    ctx_note = (
        " (an optional trailing ``ctx: TrxCtx`` parameter is also allowed)"
        if allow_trx_ctx
        else ""
    )
    # With a single output slot the lone TableFrameSpec may be returned
    # bare, without the enclosing 1-tuple -- surface that alternative so
    # the error block is not misleading.
    single_out_note = (
        "\n    # a single output may also be returned bare, without the "
        f"1-tuple:\n    def {name}(...) -> TableFrameSpec:\n        ..."
        if output_count == 1
        else ""
    )

    return (
        f"\n\nExpected signature ({input_count} input slot(s), "
        f"{output_count} output slot(s); slot type "
        f"= {_slot_mode_hint(input_mode)}{ctx_note}):\n"
        f"    def {name}(\n"
        f"        {params},\n"
        f"    ) -> tuple[{out_args}]:\n"
        f"        ...{single_out_note}"
    )


def _is_trx_ctx_slot(t: Any) -> bool:
    # `TrxCtx` lives in `tabsdatak.api`, which imports from this
    # module -- defer the import to avoid a load-time cycle.
    from tabsdatak.api import TrxCtx

    return isinstance(t, type) and issubclass(t, TrxCtx)


def has_trx_ctx_param(func: Callable[..., Any]) -> bool:
    """True if ``func``'s last parameter is annotated as ``TrxCtx`` (or a
    subclass). The runtime uses this to decide whether to pass a
    transaction-context instance when invoking the function."""
    try:
        hints = typing.get_type_hints(func)
    except Exception:
        return False
    sig = inspect.signature(func)
    params = list(sig.parameters.values())
    if not params:
        return False
    last_hint = hints.get(params[-1].name)
    return last_hint is not None and _is_trx_ctx_slot(last_hint)


def check_callable_signature(
    func: Callable[..., Any],
    *,
    input_count: int,
    input_mode: str,
    output_count: int,
    allow_trx_ctx: bool = True,
) -> None:
    """Validate a decorated function's signature against the
    declared input / output shape.

    :param input_count: Number of input-slot parameters.
    :param input_mode: ``"single"`` (each slot is a
        ``TableFrameSpec``), ``"array"`` (each slot is a
        ``TableFramesSpec``), or ``"any"`` (either).
    :param output_count: Number of return-tuple elements.
    :param allow_trx_ctx: When ``True`` (the default for
        ``@publisher`` / ``@transformer`` / ``@subscriber``), a
        single trailing ``ctx: TrxCtx`` parameter is allowed after
        the input slots. ``@stream_publisher`` passes ``False`` --
        stream publishers run inside the runtime's long-lived
        plugin loop, not per-trx, so no ``TrxCtx`` is available
        at the call boundary.
    """
    name = getattr(func, "__name__", repr(func))
    expected = _format_expected_signature(
        name,
        input_count=input_count,
        input_mode=input_mode,
        output_count=output_count,
        allow_trx_ctx=allow_trx_ctx,
    )

    try:
        hints = typing.get_type_hints(func)
    except Exception as e:
        raise ValueError(f"{name!r}: cannot resolve type hints: {e}{expected}") from e

    sig = inspect.signature(func)
    params = list(sig.parameters.values())

    # Accepted shape:
    #   - exactly ``input_count`` slot params, or
    #   - when ``allow_trx_ctx`` is True, ``input_count`` slot
    #     params followed by one optional trailing param
    #     annotated as ``TrxCtx`` (or a subclass).
    if len(params) < input_count:
        raise ValueError(
            f"{name!r}: must declare at least {input_count} parameter(s) "
            f"(got {len(params)}){expected}"
        )
    max_params = input_count + (1 if allow_trx_ctx else 0)
    if len(params) > max_params:
        if allow_trx_ctx:
            raise ValueError(
                f"{name!r}: declares {len(params)} parameter(s) but only "
                f"{input_count} input slot(s) plus an optional trailing "
                f"TrxCtx parameter are allowed{expected}"
            )
        raise ValueError(
            f"{name!r}: declares {len(params)} parameter(s) but only "
            f"{input_count} input slot(s) are allowed (a trailing "
            f"TrxCtx parameter is not supported here){expected}"
        )
    if allow_trx_ctx and len(params) == input_count + 1:
        extra = params[input_count]
        extra_hint = hints.get(extra.name)
        if extra_hint is None or not _is_trx_ctx_slot(extra_hint):
            rendered = (
                _render_type(extra_hint) if extra_hint is not None else "unannotated"
            )
            raise ValueError(
                f"{name!r}: trailing parameter {extra.name!r} is "
                f"annotated as {rendered!r} "
                f"but must be a TrxCtx subclass{expected}"
            )

    # Each of the first input_count parameters must have a slot-type annotation.
    for i in range(input_count):
        p = params[i]
        hint = hints.get(p.name)
        if hint is None:
            raise ValueError(
                f"{name!r}: parameter {i + 1} ({p.name!r}) must be annotated "
                f"as a {input_mode}-mode slot type "
                f"({_slot_mode_hint(input_mode)}){expected}"
            )
        if not _matches_slot_mode(hint, input_mode):
            raise ValueError(
                f"{name!r}: parameter {i + 1} ({p.name!r}) is annotated as "
                f"{_render_type(hint)!r} but must be a {input_mode}-mode "
                f"slot ({_slot_mode_hint(input_mode)}){expected}"
            )

    # Return shape:
    #   - canonically a fixed-length ``tuple`` of exactly
    #     ``output_count`` single-mode slots; or
    #   - as a single-output convenience, when ``output_count == 1``
    #     the lone output may be returned bare -- a ``TableFrameSpec``
    #     (``TableFrame | None``) without the enclosing 1-tuple.
    ret_hint = hints.get("return")
    if ret_hint is None:
        raise ValueError(f"{name!r}: return type must be annotated{expected}")

    if typing.get_origin(ret_hint) is not tuple:
        # Single-output convenience: accept a bare TableFrameSpec
        # when exactly one output slot is declared.
        if output_count == 1 and _is_single_slot(ret_hint):
            return
        raise ValueError(
            f"{name!r}: return type must be a tuple of {output_count} "
            f"TableFrame element(s) (got {_render_type(ret_hint)}){expected}"
        )
    ret_args = typing.get_args(ret_hint)
    # tuple[()] -> ((),), variable-length -> (X, Ellipsis); both are
    # rejected. Only fixed-length tuple shapes pass.
    if ret_args == ((),):
        ret_arity = 0
    elif len(ret_args) == 2 and ret_args[1] is Ellipsis:
        raise ValueError(
            f"{name!r}: return tuple must have a fixed length of "
            f"{output_count} (got variable-length tuple[..., ...])"
            f"{expected}"
        )
    else:
        ret_arity = len(ret_args)

    if ret_arity != output_count:
        raise ValueError(
            f"{name!r}: return tuple must have {output_count} "
            f"element(s), got {ret_arity}{expected}"
        )

    # Each return element is a single-mode slot
    # (``TableFrameSpec`` == ``TableFrame | None``).
    for i, t in enumerate(ret_args):
        if not _is_single_slot(t):
            raise ValueError(
                f"{name!r}: return tuple element {i + 1} is "
                f"{_render_type(t)!r} but must be TableFrameSpec "
                f"(TableFrame | None){expected}"
            )


# ---------------------------------------------------------------------------
# Envelope-based JSON serde for tabsdatak API objects.
#
# Every public class extending `_JsonSerde` (typically via the abstract
# bases `Conn` / `Src` / `Dest` / `Trigger` / `OnTablesAction`, all of
# which derive from `_DescriptorBase` below) gains:
#
#   * `to_dict()` / `to_json()`
#   * classmethod `from_dict(d)` / `from_json(s)`
#
# The on-wire envelope is::
#
#     {"kind":       "<resource kind>",
#      "apiVersion": "<schema version>",
#      "type":       "<module>:<qualname>",
#      "spec":       { ... fields ... }}
#
# On deserialization, `type` is resolved via `importlib.import_module`
# plus attribute walk. The resolved class must be a subclass of the
# class `from_dict` was invoked on -- this prevents arbitrary-class
# instantiation from untrusted input. `kind` is also matched against
# the target class's `__kind__` so a payload aimed at the wrong slot
# is rejected before any class is resolved.
#
# Polymorphic fields work transparently: pydantic serializes through
# the declared type's schema, our wrap-serializer detects the inner
# result is already an envelope (because the actual class's schema
# also wrapped) and avoids double-wrapping.
#
# Schema evolution is handled by `__migrations__`: a chain keyed by
# `from_version`, mapping to `(to_version, fn)`. On load, if the
# envelope's `apiVersion` differs from the class's `__api_version__`,
# the chain is walked to upgrade the spec dict before validation.
# Cycles and dead-ends raise `ValueError`.
#
# Implementation notes
# --------------------
# Both validation and serialization are installed via
# `__get_pydantic_core_schema__`, not via `@model_validator` /
# `@model_serializer` decorators. The decorator-based path is only
# honored for classes pydantic itself builds (`BaseModel` / pydantic
# dataclass); the schema-based path fires uniformly for stdlib
# `@dataclass` subclasses too.
# ---------------------------------------------------------------------------


_ENVELOPE_KEYS = frozenset({"kind", "apiVersion", "type", "spec"})


class JsonSerde:
    """Mixin providing envelope-based JSON round-trip
    (:meth:`to_json` / :meth:`from_json`) for framework dataclasses.

    Each subclass sets ``__kind__`` (the on-wire discriminator) and
    optionally ``__api_version__`` + ``__migrations__`` for schema
    evolution. Polymorphic loads validate that the envelope's
    ``type`` resolves to a subclass of the receiving class --
    arbitrary class loading from untrusted input is rejected."""

    __kind__: ClassVar[str] = "unknown"
    __api_version__: ClassVar[str] = "1.0"
    __migrations__: ClassVar[dict[str, tuple[str, Callable[[dict], dict]]]] = {}
    # Set by the `dataclass` decorator on every concrete descriptor.
    __api_error__: ClassVar[ErrorCode | None] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return TypeAdapter(type(self)).dump_python(self, mode="json")

    def to_json(self, indent: int | None = None) -> str:
        return TypeAdapter(type(self)).dump_json(self, indent=indent).decode()

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Any:
        with cls._tagged_errors():
            return TypeAdapter(cls).validate_python(d)

    @classmethod
    def from_json(cls, s: str | bytes) -> Any:
        with cls._tagged_errors():
            return TypeAdapter(cls).validate_json(s)

    @classmethod
    @contextlib.contextmanager
    def _tagged_errors(cls) -> Any:
        """Translate a pydantic ``ValidationError`` into this descriptor's
        `ErrorCode`, mirroring what the `dataclass` decorator does for
        construction, so both paths surface the same exception family."""
        try:
            yield
        except ValidationError as e:
            if cls.__api_error__ is None:
                raise
            raise cls.__api_error__.exception(cause=e) from e

    # ------------------------------------------------------------------
    # Pydantic schema hook (inherited by every subclass)
    # ------------------------------------------------------------------

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        # Default schema: dataclass / model schema for concrete classes,
        # is_instance fallback for plain ABCs that pydantic cannot
        # generate a schema for. Narrow `except` so genuine bugs in
        # `handler(...)` still surface.
        try:
            inner = handler(source_type)
        except PydanticSchemaGenerationError:
            inner = core_schema.is_instance_schema(cls)

        def validate(value: Any, h: Callable[[Any], Any]) -> Any:
            # Pre-built instance: pass through.
            if isinstance(value, cls):
                return value
            # Envelope shape: resolve, kind-check, migrate, dispatch.
            if isinstance(value, dict) and _ENVELOPE_KEYS <= value.keys():
                target = resolve_class(value["type"])
                if not issubclass(target, cls):
                    raise ValueError(
                        f"envelope type {value['type']!r} resolved to "
                        f"{target.__qualname__}, which is not a subclass "
                        f"of {cls.__qualname__}"
                    )
                if value["kind"] != target.__kind__:
                    raise ValueError(
                        f"envelope kind {value['kind']!r} does not match "
                        f"{target.__qualname__}.__kind__="
                        f"{target.__kind__!r}"
                    )
                spec = _migrate(value["spec"], value["apiVersion"], target)
                # An envelope is a wire document: validate its spec in
                # json-mode so string-or-secret / Secret leaves decode their
                # ``str:`` / ``secret:`` / ``:secret:`` markers (python-mode
                # would keep them as literal strings).
                return TypeAdapter(target).validate_json(json.dumps(spec))
            # Anything else (e.g. plain field dict produced by a parent
            # wrap-validator that already resolved the envelope): defer
            # to default validation.
            return h(value)

        def serialize(instance: Any, h: Callable[[Any], Any]) -> dict:
            # `h(instance)` invokes the inner schema's default serializer.
            # In a polymorphic field slot pydantic may chain back through
            # the *actual* class's schema, which itself wraps -- in that
            # case `spec` arrives already enveloped and we must not
            # double-wrap. Detect by structural shape and pass through.
            actual_cls = type(instance)
            # If the instance is a subclass / different concrete type than
            # the declared field type, pydantic's default chain runs through
            # the *declared* type's schema and silently drops fields the
            # subclass added (a field declared only on the subclass). Re-route
            # through the actual class's TypeAdapter so its full schema fires
            # and the result includes every subclass-declared field.
            if actual_cls is not cls:
                return TypeAdapter(actual_cls).dump_python(instance, mode="json")
            spec = h(instance)
            if isinstance(spec, dict) and _ENVELOPE_KEYS <= spec.keys():
                return spec
            return {
                "kind": actual_cls.__kind__,
                "apiVersion": actual_cls.__api_version__,
                "type": qualified_class(actual_cls),
                "spec": spec,
            }

        return core_schema.no_info_wrap_validator_function(
            validate,
            inner,
            serialization=core_schema.wrap_serializer_function_ser_schema(
                serialize, info_arg=False
            ),
        )


class DescriptorBase(JsonSerde):
    """Common ancestor for framework-described user types (Trigger,
    Conn, Src, StreamSrc, Dest, ...). Layers JSON round-trip on top
    of ``JsonSerde`` and provides a ``validate()`` hook that runs
    after construction, in addition to the pydantic dataclass
    field-level validation."""

    def validate(self) -> None:
        pass

    @model_validator(mode="after")
    def _post_init_hooks(self):
        check_tz_aware_datetime(self)
        self.validate()
        return self


# ---------------------------------------------------------------------------
# Class / callable resolution helpers
# ---------------------------------------------------------------------------


# Mirrors ``tabsdatak.common._archive.bundle.BUNDLE_USER_CODE_DIRNAME``.
# Defined locally so ``tabsdatak-api`` need not depend on ``tabsdatak-common``
# for a single constant; keep the two in sync.
_BUNDLE_USER_CODE_DIRNAME = "user_code"


def _load_bare_module_from_folder(mod_name: str, folder: str) -> Any:
    """Load bare ``mod_name`` from ``folder`` as a submodule of a synthetic
    package rooted at that folder (the CLI register scheme), so relative
    imports resolve and the module never shadows / re-imports an installed
    package under its bare name. Returns ``None`` if ``folder`` has no
    matching ``<name>.py`` / ``<name>/`` package."""
    file_path = os.path.join(folder, f"{mod_name}.py")
    package_init = os.path.join(folder, mod_name, "__init__.py")
    is_package = os.path.isfile(package_init)
    if not (os.path.isfile(file_path) or is_package):
        return None
    folder_path = os.path.abspath(folder)
    folder_digest = hashlib.sha1(folder_path.encode("utf-8")).hexdigest()[:12]
    package_name = f"td_usr_pkg_{folder_digest}"
    synthetic_name = f"{package_name}.{mod_name}"
    if synthetic_name in sys.modules:
        return sys.modules[synthetic_name]
    if package_name not in sys.modules:
        package_spec = importlib.machinery.ModuleSpec(
            package_name, loader=None, is_package=True
        )
        package_spec.submodule_search_locations = [folder_path]
        sys.modules[package_name] = importlib.util.module_from_spec(package_spec)
    location = package_init if is_package else file_path
    spec = importlib.util.spec_from_file_location(
        synthetic_name,
        location,
        submodule_search_locations=(
            [os.path.join(folder_path, mod_name)] if is_package else [folder_path]
        ),
    )
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    sys.modules[synthetic_name] = module
    spec.loader.exec_module(module)
    return module


def _import_user_module(mod_name: str) -> Any:
    """Import ``mod_name`` for callable / class resolution.

    A bundled user module is recorded bare (``"publisher"``, no package)
    and its siblings sit flat next to it in ``user_code/``, which the
    runner has prepended to ``sys.path``. Resolution handles three cases:

    * **name collision with an installed package** -- a user file named
      after an installed package (``databricks.py``, ``snowflake.py``,
      ``kafka.py``, ``json.py`` ...) sitting in a bundle ``user_code/``
      folder is preferred over the installed package, because register
      already committed to treating it as the user's file (that is why it
      recorded the name bare). Scoped to ``user_code/``-named folders so
      it never re-imports an installed package (e.g.
      ``site-packages/polars``) under a synthetic name.
    * the common case -- a plain ``importlib.import_module`` resolves
      installed deps, dotted names, and non-colliding user modules.
    * **relative imports** -- a plainly-imported top-level module has no
      parent package, so ``from .supporter import ...`` inside it raises
      "attempted relative import with no known parent package"; it is
      retried through a synthetic package rooted at the folder holding the
      file, which gives the relative import a parent to resolve against.
    """
    if "." not in mod_name:
        for entry in sys.path:
            folder = entry or os.getcwd()
            if os.path.basename(os.path.normpath(folder)) != _BUNDLE_USER_CODE_DIRNAME:
                continue
            module = _load_bare_module_from_folder(mod_name, folder)
            if module is not None:
                return module

    try:
        return importlib.import_module(mod_name)
    except ImportError as exc:
        if "." in mod_name or "relative import" not in str(exc):
            raise

    for entry in sys.path:
        folder = entry or os.getcwd()
        sys.modules.pop(mod_name, None)
        module = _load_bare_module_from_folder(mod_name, folder)
        if module is not None:
            return module
    return importlib.import_module(mod_name)


def qualified_class(cls: type) -> str:
    """Render ``cls`` as ``"module:qualname"`` for JSON envelope
    serialisation. Rejects locally-defined classes (qualname
    contains ``<locals>``) -- they can't be re-imported."""
    qualname = cls.__qualname__
    if "<locals>" in qualname:
        raise ValueError(
            f"cannot serialize locally-defined class "
            f"{cls.__module__}:{qualname} (must be importable at module "
            f"top level)"
        )
    return f"{cls.__module__}:{qualname}"


def resolve_class(spec: str) -> type:
    """Inverse of :func:`qualified_class` -- import the module and
    walk the qualname path. Raises ``ValueError`` on a malformed
    spec, a missing module / attribute, or a non-class target."""
    if ":" not in spec:
        raise ValueError(f"type {spec!r}: expected '<module>:<qualname>'")
    mod_name, _, qualname = spec.partition(":")
    if "<locals>" in qualname:
        raise ValueError(f"type {spec!r}: cannot resolve locally-defined class")
    try:
        mod = _import_user_module(mod_name)
    except ImportError as e:
        raise ValueError(
            f"type {spec!r}: module {mod_name!r} could not be imported: {e}"
        ) from e
    obj: Any = mod
    for part in qualname.split("."):
        try:
            obj = getattr(obj, part)
        except AttributeError as e:
            raise ValueError(
                f"type {spec!r}: attribute path not found in module {mod_name!r}"
            ) from e
    if not isinstance(obj, type):
        raise ValueError(f"type {spec!r}: did not resolve to a class (got {type(obj)})")
    return obj


def _migrate(spec: dict, from_version: str, target: type) -> dict:
    target_version = target.__api_version__  # type: ignore[attr-defined]
    migrations = target.__migrations__  # type: ignore[attr-defined]
    seen: set[str] = set()
    while from_version != target_version:
        if from_version in seen:
            raise ValueError(
                f"{target.__qualname__}: migration cycle at version {from_version!r}"
            )
        seen.add(from_version)
        try:
            to_version, fn = migrations[from_version]
        except KeyError:
            raise ValueError(
                f"{target.__qualname__}: no migration from version "
                f"{from_version!r} to {target_version!r}"
            ) from None
        spec = fn(spec)
        from_version = to_version
    return spec


def callable_to_str(func: Callable[..., Any]) -> str:
    """Render ``func`` as ``"module:qualname"`` for JSON envelope
    serialisation of ``*Info.func``. Rejects closures."""
    qualname = func.__qualname__
    if "<locals>" in qualname:
        # noinspection PyUnresolvedReferences
        raise ValueError(
            f"cannot serialize locally-defined callable "
            f"{func.__module__}:{qualname} (must be importable at module "
            f"top level)"
        )
    # noinspection PyUnresolvedReferences
    return f"{func.__module__}:{qualname}"


def callable_from_str(spec: str) -> Callable[..., Any]:
    """Inverse of :func:`callable_to_str` -- resolve a
    ``"module:qualname"`` string back to the callable."""
    if ":" not in spec:
        raise ValueError(f"callable {spec!r}: expected '<module>:<qualname>'")
    mod_name, _, qualname = spec.partition(":")
    if "<locals>" in qualname:
        raise ValueError(f"callable {spec!r}: cannot resolve locally-defined callable")
    try:
        mod = _import_user_module(mod_name)
    except ImportError as e:
        raise ValueError(
            f"callable {spec!r}: module {mod_name!r} could not be imported: {e}"
        ) from e
    obj: Any = mod
    for part in qualname.split("."):
        try:
            obj = getattr(obj, part)
        except AttributeError as e:
            raise ValueError(
                f"callable {spec!r}: attribute path not found in module {mod_name!r}"
            ) from e
    if not callable(obj):
        raise ValueError(
            f"callable {spec!r}: did not resolve to a callable (got {type(obj)})"
        )
    return obj
