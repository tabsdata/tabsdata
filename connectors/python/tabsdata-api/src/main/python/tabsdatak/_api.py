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
* :class:`JsonSerde` -- mixin providing JSON envelope round-trip
  (``to_json`` / ``from_json``) with a ``__kind__`` discriminator
  for polymorphic loading.
* :class:`DescriptorBase` -- :class:`JsonSerde` plus the tagging
  the runtime relies on.
* :class:`BaseRef` -- shared method-host for
  :class:`~tabsdatak.api.Var` / :class:`~tabsdatak.api.Secret`
  (the two leaves are structurally identical: same ``_s`` /
  ``_value_pattern`` shape, same empty-init guard, same named-ref
  factory).
* :class:`VarValueConstraints` / :class:`SecretValueConstraints` --
  field-site value-pattern constraints.
* ``*_CONSTRAINTS`` validators -- per-domain name patterns
  (table, var, cron mask, ...).
* :func:`callable_to_str` / :func:`callable_from_str` /
  :func:`qualified_class` / :func:`resolve_class` --
  ``*Info``-side helpers for JSON round-trip of functions /
  class references.
"""

from __future__ import annotations

import dataclasses
import functools
import importlib
import inspect
import re
import typing
from abc import ABC
from datetime import datetime
from typing import Any, Callable, ClassVar, Type, TypeVar, Union, dataclass_transform

from croniter import croniter
from pydantic import (
    AfterValidator,
    Field,
    GetCoreSchemaHandler,
    PrivateAttr,
    TypeAdapter,
    ValidationError,
    model_validator,
)
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic.errors import PydanticSchemaGenerationError
from pydantic_core import ArgsKwargs, core_schema

# Error codes and exceptions live in `tabsdatak.api_error` so callers
# only depend on that module when they want to react to errors.
from tabsdatak.error import ApiErrorCode, ErrorCode

_C = TypeVar("_C", bound=type)


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
        return decorated  # type: ignore[return-value]

    return wrap


# ---------------------------------------------------------------------------
# Field-level validators exposed for use in `tabsdatak.api` type aliases.
#
# Each is exposed as an `AfterValidator` constant so consumers can write:
#     SomeSpec: TypeAlias = Annotated[str, _CRONMASK_VALIDATOR]
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


def _validate_table_name(value: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(
            f"not a valid table name (1-64 chars, must start with a letter, "
            f"alphanumeric and underscore only): {value!r}"
        )
    return value


def _validate_coll_table_name(value: str) -> str:
    if not _COLL_IDENTIFIER_RE.fullmatch(value):
        raise ValueError(
            f"not a valid collection-qualified table name "
            f"(<identifier> or <identifier>/<identifier>, no whitespace): "
            f"{value!r}"
        )
    return value


def _validate_named_connection(value: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(
            f"not a valid named connection (1-64 chars, must start with a letter, "
            f"alphanumeric and underscore only): {value!r}"
        )
    return value


# --- Environment variable name. POSIX-style: must start with a letter or
# --- underscore, followed by letters / digits / underscores. Empty rejected.
_VAR_NAME_RE_STR = r"^[A-Za-z_][A-Za-z0-9_]*$"
_VAR_NAME_RE = re.compile(_VAR_NAME_RE_STR)


def _validate_var_name(value: str) -> str:
    if not _VAR_NAME_RE.fullmatch(value):
        raise ValueError(
            f"not a valid environment variable name (must start with a letter "
            f"or underscore, then letters/digits/underscores): {value!r}"
        )
    return value


# --- Kubernetes secret reference: `<secret>#<key>`.
#
# `<secret>` follows the K8s object-name rules for Secret resources --
# RFC 1123 DNS subdomain, up to 253 chars: lowercase alphanumeric, '-',
# '.', must start and end with an alphanumeric. Each dot-separated label
# is itself a DNS-1123 label.
#
# `<key>` follows the K8s data-key rules for Secret entries: any
# combination of alphanumeric (case-insensitive), '-', '_', '.'. This is
# why a typical reference looks like `db-password#DB_PASSWORD` -- the
# secret name is lowercase-DNS, but the key inside it is conventionally
# uppercase to match shell var conventions.
_SECRET_REF_SEP = "#"
_DNS_1123_LABEL_RE_STR = r"[a-z0-9]([-a-z0-9]*[a-z0-9])?"
_DNS_1123_SUBDOMAIN_RE = re.compile(
    rf"^{_DNS_1123_LABEL_RE_STR}(\.{_DNS_1123_LABEL_RE_STR})*$"
)
_SECRET_DATA_KEY_RE_STR = r"^[-._a-zA-Z0-9]+$"
_SECRET_DATA_KEY_RE = re.compile(_SECRET_DATA_KEY_RE_STR)
_DNS_1123_SUBDOMAIN_MAX = 253


def _validate_secret_name(value: str) -> str:
    if _SECRET_REF_SEP not in value:
        raise ValueError(
            f"not a valid secret reference "
            f"(expected '<secret>{_SECRET_REF_SEP}<key>'): {value!r}"
        )
    secret, _, key = value.partition(_SECRET_REF_SEP)
    if not (1 <= len(secret) <= _DNS_1123_SUBDOMAIN_MAX):
        raise ValueError(
            f"secret name must be 1-{_DNS_1123_SUBDOMAIN_MAX} chars "
            f"(got {len(secret)}): {value!r}"
        )
    if not _DNS_1123_SUBDOMAIN_RE.fullmatch(secret):
        raise ValueError(
            f"secret name {secret!r} is not a valid RFC 1123 DNS "
            f"subdomain (lowercase alphanumeric, '-', '.', must start "
            f"and end with alphanumeric)"
        )
    if not _SECRET_DATA_KEY_RE.fullmatch(key):
        raise ValueError(f"secret key {key!r} must match '[-._a-zA-Z0-9]+'")
    return value


def _validate_cron_mask(value: str) -> str:
    if value in _K8S_CRON_DESCRIPTORS:
        return value
    if not _UNIX_CRON_RE.fullmatch(value):
        raise ValueError(f"not a Kubernetes CronJob schedule: {value!r}")
    if not croniter.is_valid(value):
        raise ValueError(f"invalid cron expression: {value!r}")
    return value


TABLE_NAME_CONSTRAINTS = AfterValidator(_validate_table_name)
COLL_TABLE_NAME_CONSTRAINTS = AfterValidator(_validate_coll_table_name)
CRON_MASK_CONSTRAINTS = AfterValidator(_validate_cron_mask)
NAMED_CONN_CONSTRAINTS = AfterValidator(_validate_named_connection)
VAR_NAME_CONSTRAINTS = AfterValidator(_validate_var_name)
SECRET_NAME_CONSTRAINTS = AfterValidator(_validate_secret_name)

NON_EMPTY_LIST_CONSTRAINTS = Field(min_length=1)


# ---------------------------------------------------------------------------
# Pattern annotation marker for Var / Secret fields.
#
# Use as ``Annotated[Secret, pattern(USER_PATTERN)]`` (or with Var). The
# regex string is injected into the validated instance's
# ``_value_pattern`` so it travels through serialization and is enforced
# at ``value()`` time. For ``LiteralEnv`` / ``LiteralSecret`` (literal
# values), the pattern is also enforced eagerly at construction and on
# deserialization, so a value that does not match the pattern is rejected
# at the boundary rather than at first-use.
#
# A bare ``str`` at the field site is stream-promoted to the literal sibling
# class with the pattern injected -- so callers can write
# ``MyConn(user="alice")`` instead of ``MyConn(user=Secret.literal("alice"))``.
# ---------------------------------------------------------------------------


class _ValuePattern:
    """Base for the :class:`AfterValidator` adapters that pin a
    regex to the *resolved* value of an :class:`Var` /
    :class:`Secret` field.

    Subclasses (:class:`VarValueConstraints`,
    :class:`SecretValueConstraints`) override :meth:`_on_class` to
    declare which value-reference family they cover. The validator
    runs at pydantic schema time -- when a bare string is passed
    where the field expects an Var / Secret, it is auto-wrapped as
    the matching literal subclass with the pattern injected.
    """

    def __init__(self, *, pattern: str):
        if not isinstance(pattern, str):
            raise TypeError(f"pattern() expects a regex string, got {type(pattern)!r}")
        self._pattern = pattern

    def __repr__(self) -> str:
        return f"pattern({self._pattern!r})"

    def _on_class(self) -> Type:
        # Subclass hook -- defers `tabsdatak.api` import until the field
        # schema is built (well after module load), so a module-level
        # ``EnvValuePattern(...)`` literal in this module does not race
        # the api <-> _api import cycle.
        raise NotImplementedError

    def __get_pydantic_core_schema__(
        self,
        source_type: Any,
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:

        if not (
            isinstance(source_type, type) and issubclass(source_type, self._on_class())
        ):
            raise TypeError(
                f"pattern() can only annotate Var or Secret subclasses "
                f"(got {source_type!r})"
            )

        # Late import: api.py imports from _api at module load, and these
        # classes are defined there.
        # noinspection PyProtectedMember
        from tabsdatak.api import Secret, Var, _LiteralSecret, _LiteralVar

        if issubclass(source_type, Var):
            literal_cls: type = _LiteralVar
            field_name = "_s"
        elif issubclass(source_type, Secret):
            literal_cls: type = _LiteralSecret
            field_name = "_s"
        else:
            raise TypeError()

        regex = self._pattern
        base = handler(source_type)

        def wrap(value: Any, h: Callable[[Any], Any]) -> Any:
            # Bare-string shortcut: build a literal with the pattern
            # injected. The literal's own model_validator enforces the
            # pattern against the value at construction.
            if isinstance(value, str):
                return literal_cls(**{field_name: value, "_value_pattern": regex})

            instance = h(value)
            # Skip reconstruction when the instance already carries the
            # field's regex. True on round-trips through the *same*
            # annotated field (the JSON spec carries `_value_pattern`,
            # so `h(value)` rebuilds an instance with it already set),
            # or when the caller explicitly built the instance with the
            # matching regex. False -- and we reconstruct -- in the
            # common case: a fresh `Var(s=...)` at the call site,
            # cross-field reuse with a different regex, or legacy
            # payloads that lack `_value_pattern`.
            if getattr(instance, "_value_pattern", None) == regex:
                return instance

            # Reconstruct so the field's pattern wins, and the literal
            # model_validator runs against the field's pattern.
            # ``Secret.__init__`` is empty-only (the user-facing guard
            # in ``api.py``); route via the pydantic validator so the
            # framework reconstruction is not subject to that guard.
            return type(instance).__pydantic_validator__.validate_python({
                field_name: getattr(instance, field_name),
                "_value_pattern": regex,
            })

        return core_schema.no_info_wrap_validator_function(wrap, base)


class VarValueConstraints(_ValuePattern):
    """Field-site value-pattern check for
    :class:`~tabsdatak.api.Var` references. Use as
    ``Annotated[Var, VarValueConstraints(pattern=...)]`` on a
    per-connector ``UriSpec`` / credential alias."""

    def _on_class(self) -> Type:
        from tabsdatak.api import Var

        return Var


VAR_VALUE_NAMED_CONN_CONSTRAINTS = VarValueConstraints(pattern=_VAR_NAME_RE_STR)


class SecretValueConstraints(_ValuePattern):
    """Field-site value-pattern check for
    :class:`~tabsdatak.api.Secret` references. Use as
    ``Annotated[Secret, SecretValueConstraints(pattern=...)]``
    on a per-connector ``UriSpec`` / credential alias."""

    def _on_class(self) -> Type:
        from tabsdatak.api import Secret

        return Secret


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
#     ``ctx.get_attr`` / ``ctx.store_attr`` to read state from the
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

    return (
        f"\n\nExpected signature ({input_count} input slot(s), "
        f"{output_count} output slot(s); slot type "
        f"= {_slot_mode_hint(input_mode)}{ctx_note}):\n"
        f"    def {name}(\n"
        f"        {params},\n"
        f"    ) -> tuple[{out_args}]:\n"
        f"        ..."
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

    # Return: tuple of exactly output_count single-mode slots.
    ret_hint = hints.get("return")
    if ret_hint is None:
        raise ValueError(f"{name!r}: return type must be annotated{expected}")

    if typing.get_origin(ret_hint) is not tuple:
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

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return TypeAdapter(type(self)).dump_python(self, mode="json")

    def to_json(self, indent: int | None = None) -> str:
        return TypeAdapter(type(self)).dump_json(self, indent=indent).decode()

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Any:
        return TypeAdapter(cls).validate_python(d)

    @classmethod
    def from_json(cls, s: str | bytes) -> Any:
        return TypeAdapter(cls).validate_json(s)

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
                return TypeAdapter(target).validate_python(spec)
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
            # subclass added (e.g. LiteralEnv.val at an Var field). Re-route
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
# Method-host shared by `tabsdatak.api.Var` and `tabsdatak.api.Secret`.
#
# Both leaves are structurally identical: each pairs an ``init=False``
# ``_s`` slot with an optional ``_value_pattern`` regex, each is built
# bare from user code (``Var()`` / ``Secret()``), and each gets its
# real name populated either by the framework during envelope
# round-trip or by the matching ``_<role>_key`` factory. The shared
# behaviour -- ``_name`` / ``_value`` / ``_validate`` / ``value``, the
# pydantic empty-init guard, and the named-ref factory -- lives here
# once. Public surface stays split: ``Var`` and ``Secret`` are
# first-class classes in ``api.py`` and remain distinct user-facing
# concepts.
# ---------------------------------------------------------------------------


class BaseRef(JsonSerde, ABC):
    """Shared method-host for :class:`~tabsdatak.api.Var` and
    :class:`~tabsdatak.api.Secret`.

    Leaves customise three class attributes:

    * ``_no_resolver_error`` -- :class:`ApiErrorCode` raised by
      :meth:`_value` when no resolver is wired up. ``API_8`` for
      ``Var`` (runtime), ``API_11`` for ``Secret`` (internal).
    * ``_is_ref_root`` -- ``True`` on the user-facing ref class
      (``Var`` / ``Secret``), ``False`` on the literal subclass.
      Gates the empty-init guard and the dict-input re-injection
      path; literal subclasses redeclare their fields with
      ``init=True`` and need neither.
    * ``_role`` -- the role-specific factory name (``"_var_key"`` /
      ``"_secret_key"``) used in the error message produced when
      the guard rejects a non-empty user init."""

    # Defaults exist so subclasses can selectively override. Declared
    # without annotations to keep pydantic from treating them as fields.
    _no_resolver_error = None  # set by leaf
    _is_ref_root = False
    _role = "_key"

    def _name(self) -> str:
        return self._s  # type: ignore[attr-defined]

    def _value(self) -> str:
        # No resolver wired up by default: refs only resolve under a
        # role-specific integration that is not part of the API
        # package (env-var lookup for ``Var``, secret-store lookup
        # for ``Secret``).
        raise type(self)._no_resolver_error.exception(name=self._s)  # type: ignore[attr-defined]

    def _validate(self, val: str) -> str:
        pattern = self._value_pattern  # type: ignore[attr-defined]
        if pattern is not None and re.fullmatch(pattern, val) is None:
            raise ApiErrorCode.API_6.exception(
                name=self._name(),
                pattern=pattern,
                value=val,
            )
        return val

    def value(self) -> str:
        return self._validate(self._value())

    @classmethod
    def _key(cls, name: str) -> Any:
        # The leaf's private fields are ``init=False`` so they cannot
        # be set from the user-visible ``__init__``; route through
        # the pydantic validator instead, where the dict-input path
        # re-injects them after the (empty) handler runs.
        return cls.__pydantic_validator__.validate_python(  # type: ignore[attr-defined]
            {"_s": name, "_value_pattern": cls._value_pattern}  # type: ignore[attr-defined]
        )

    @model_validator(mode="wrap")
    @classmethod
    def _enforce_empty_user_init(cls, data: Any, handler) -> Any:
        # Two-way guard wrapping pydantic's standard validation:
        #
        # 1. ``ArgsKwargs`` arrives only from the user-visible
        #    ``__init__`` site. On a ref root (``Var`` / ``Secret``)
        #    it must be empty; anything else is a misuse. The
        #    literal subclass overrides its fields with ``init=True``
        #    and sets ``_is_ref_root = False`` so it bypasses this
        #    branch.
        #
        # 2. Dict input arrives from envelope round-trip and the
        #    ``_key`` factory. The ref root's private fields are
        #    ``init=False`` so the standard validator drops them; we
        #    stash them before the handler runs and
        #    ``object.__setattr__`` them back afterwards.
        if isinstance(data, ArgsKwargs):
            if cls._is_ref_root and (data.args or data.kwargs):
                role = cls.__qualname__
                raise ValueError(
                    f"{role}() takes no arguments; "
                    f"use {role}.lit(value) for inline values, or "
                    f"{role}.{cls._role}(name) for a named reference"
                )
            return handler(data)

        if cls._is_ref_root and isinstance(data, dict):
            stash = {k: data[k] for k in ("_s", "_value_pattern") if k in data}
            instance = handler(data)
            for k, v in stash.items():
                object.__setattr__(instance, k, v)
            return instance

        return handler(data)


# ---------------------------------------------------------------------------
# Class / callable resolution helpers
# ---------------------------------------------------------------------------


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
        mod = importlib.import_module(mod_name)
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
        mod = importlib.import_module(mod_name)
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
