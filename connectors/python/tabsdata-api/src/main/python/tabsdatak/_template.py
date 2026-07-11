#
# Copyright 2026 Tabs Data Inc.
#

"""Connection-template engine for the ``tdk connection`` CLI.

Introspects the connectors registered through the SPI registry
(:func:`tabsdatak._connectors.avail_srcs` / ``avail_stream_srcs`` /
``avail_dests``) and emits the connectors' own envelope dialect as YAML::

    # Connection template for: mysql-sql-in (src)
    # Secret values must be prefixed with 'secret:'.
    kind: connectionDef
    apiVersion: '1.0'
    type: tabsdatak.conn.mysql:MySQLSrcConn
    spec:

      # accepts: <value> | secret:<value>
      # replace <value> with the actual uri
      #
      uri: '<value> or secret:<value>'
      credentials:           # ... nested userPasswordCredentials envelope ...
        ...

The ``spec`` mirrors the connection :class:`~tabsdatak.spi.Conn` dataclass's
pydantic fields. Each fill-in field renders a ``<value>`` placeholder generated
from the **first** member of its type union:

- a :class:`~tabsdatak.api.Secret` first member renders as ``'secret:<value>'``
  (or ``'<value> or secret:<value>'`` when the union also accepts a plain
  literal); the runtime store resolves the value the user fills in;
- a nested-object first member recurses into its own envelope;
- any other first member renders ``'<value>'`` or a concrete typed sample
  (enum / int / bool / a field carrying a default).

Every fill-in field carries a comment block above it: a blank-line separator,
an optional ``# accepts: ...`` line (only when the union accepts more than one
form, e.g. uri / bucket / region), a ``# replace <value> ...`` hint, then a
bare ``#`` border just above the key. Fields rendering a concrete sample (a
default, enum, ...) carry no block.

[`build_template`][build_template] / [`build_template_interactive`] return a
YAML-ready :class:`~ruamel.yaml.comments.CommentedMap`; [`to_yaml`] renders it
(comments included) and [`from_yaml`] parses a document back (YAML is a superset
of JSON, so JSON parses too).

No Click, no terminal I/O: the interactive builder takes the prompt function as a
callback. ``validate`` constructs the concrete connection via the connector's own
``from_dict`` -- the full pydantic validation, kind/type cross-check, and
field-pattern enforcement.
"""

from __future__ import annotations

import enum as _enum
import io
import types as _types
import typing
from dataclasses import dataclass
from typing import Any, Callable, Optional

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.error import CommentMark
from ruamel.yaml.scalarstring import SingleQuotedScalarString
from ruamel.yaml.tokens import CommentToken

# noinspection PyProtectedMember
from tabsdatak._api import DescriptorBase, qualified_class, resolve_class
from tabsdatak._connectors import avail_dests, avail_srcs, avail_stream_srcs
from tabsdatak.api import Secret
from tabsdatak.spi import Conn

# pydantic marks a required field's default with a sentinel; treat it as unset.
try:
    from pydantic_core import PydanticUndefined as _UNSET
except Exception:  # pragma: no cover - import shape differs across builds
    _UNSET = object()

_NONE = type(None)
_ENVELOPE_KEYS = frozenset({"kind", "apiVersion", "type", "spec"})

Direction = str  # one of _DIRECTIONS
_DIRECTIONS = ("src", "stream-src", "dest")


class TemplateError(Exception):
    """User-facing error (unknown connector, malformed document). Rendered
    verbatim by the CLI."""


# Human-readable rendering of a connector's direction, shown by
# ``tdk connection types`` as the "Mode" column.
_MODE_BY_DIRECTION = {
    "src": "Source",
    "stream-src": "Stream Source",
    "dest": "Destination",
}


@dataclass(frozen=True)
class ConnectorInfo:
    """One registered connector binding: its wire type, direction, version,
    target system and the concrete :class:`~tabsdatak.spi.Conn` subclass to
    introspect."""

    type: str
    direction: Direction
    version: str
    system: str
    conn_cls: type

    @property
    def kind(self) -> str:
        return self.conn_cls.__kind__

    @property
    def mode(self) -> str:
        """Human-readable direction (``Source`` / ``Destination`` /
        ``Stream Source``)."""
        return _MODE_BY_DIRECTION[self.direction]


# ---------------------------------------------------------------------------
# Discovery -- strictly via the SPI registry.
# ---------------------------------------------------------------------------


def _user_facing(conn_cls: type) -> bool:
    """A real connection class: a pydantic-introspectable connection that
    declares its own config fields. Excludes internal field-less bindings
    like the stream-stage publisher's ``_StreamStageSrcConn`` (every
    connection now shares the same ``connectionDef`` kind, so the kind no
    longer distinguishes them)."""
    return hasattr(conn_cls, "__pydantic_fields__")


def list_connectors() -> list[ConnectorInfo]:
    """Every installed user-facing connector, flattened from the three SPI
    registries."""
    rows: list[ConnectorInfo] = []
    for type_, d in avail_srcs().items():
        rows.append(ConnectorInfo(type_, "src", d.src_version, d.system, d.conn))
    for type_, d in avail_stream_srcs().items():
        rows.append(ConnectorInfo(type_, "stream-src", d.src_version, d.system, d.conn))
    for type_, d in avail_dests().items():
        rows.append(ConnectorInfo(type_, "dest", d.dest_version, d.system, d.conn))
    rows = [r for r in rows if _user_facing(r.conn_cls)]
    rows.sort(key=lambda r: (r.type, r.direction))
    return rows


def resolve_conn(type_: str, direction: Optional[str]) -> ConnectorInfo:
    """Resolve a connector ``type`` (+ optional ``direction``) to its binding.

    Infers the direction when the type is unambiguous; raises a clear
    :class:`TemplateError` for an unknown type, an absent direction on an
    ambiguous type, or a direction the type does not offer."""
    if direction is not None and direction not in _DIRECTIONS:
        raise TemplateError(
            f"unknown direction {direction!r}; expected one of {', '.join(_DIRECTIONS)}"
        )
    rows = list_connectors()
    matches = [r for r in rows if r.type == type_]
    if not matches:
        types_ = ", ".join(sorted({r.type for r in rows}))
        raise TemplateError(f"unknown connector type {type_!r}; available: {types_}")
    if direction is not None:
        for r in matches:
            if r.direction == direction:
                return r
        offered = ", ".join(sorted(r.direction for r in matches))
        raise TemplateError(
            f"connector {type_!r} has no {direction!r} direction; it offers: {offered}"
        )
    if len(matches) == 1:
        return matches[0]
    offered = ", ".join(sorted(r.direction for r in matches))
    raise TemplateError(
        f"connector {type_!r} is ambiguous; specify -d one of: {offered}"
    )


# ---------------------------------------------------------------------------
# Annotation analysis.
# ---------------------------------------------------------------------------


def _members(ann: Any) -> list[Any]:
    """Union members with ``Annotated`` peeled and ``None`` dropped (covers
    both ``typing.Union`` and PEP-604 ``X | Y``)."""
    if typing.get_origin(ann) in (typing.Union, _types.UnionType):
        raw = [a for a in typing.get_args(ann) if a is not _NONE]
    else:
        raw = [ann]
    out = []
    for m in raw:
        base = m
        while typing.get_origin(base) is typing.Annotated:
            base = typing.get_args(base)[0]
        out.append(base)
    return out


def _ref_class(first: Any) -> Optional[type]:
    """If the union's first member is a :class:`~tabsdatak.api.Secret`
    reference type, return it (else ``None``).
    Drives both the static sample and the wizard's secret-style prompt."""
    if isinstance(first, type) and issubclass(first, Secret):
        return first
    return None


def _type_comment(members: list[Any]) -> Optional[str]:
    """A one-line note listing the forms a field accepts, in the union's
    declaration order -- e.g. ``Union[str, Secret]`` (uri, bucket, region)
    -> ``accepts: <value> | secret:<value>``. ``None`` for a field
    that accepts a single form (a plain scalar, a lone ``Secret`` credential, or
    a nested object carries no such note)."""
    has_ref = any(isinstance(m, type) and issubclass(m, Secret) for m in members)
    if not has_ref:
        return None
    forms: list[str] = []
    for m in members:
        if isinstance(m, type) and issubclass(m, Secret):
            forms.append("secret:<value>")
        else:
            forms.append("<value>")
    if len(forms) <= 1:
        return None
    return "accepts: " + " | ".join(forms)


def _concrete_subs(cls: type) -> list[type]:
    """Introspectable concrete descendants of a nested field type -- those
    carrying their own pydantic fields (an abstract base like ``CloudLocation``
    or ``GCPCredentials`` defines none; its subclasses do)."""
    out: list[type] = []
    if hasattr(cls, "__pydantic_fields__"):
        out.append(cls)
    for sub in cls.__subclasses__():
        out.extend(_concrete_subs(sub))
    return out


def _first_concrete(cls: type) -> type:
    """A concrete, field-bearing class for a nested field type; ``cls`` itself
    when it already carries fields."""
    subs = _concrete_subs(cls)
    if not subs:
        raise TemplateError(f"no concrete subclass for type {cls.__name__}")
    return subs[0]


def _pattern_of(ann: Any) -> Optional[str]:
    """The first regex pattern declared on the annotation's ``Annotated``
    metadata (``StringConstraints``), for wizard display."""
    candidates = (
        list(typing.get_args(ann))
        if typing.get_origin(ann) in (typing.Union, _types.UnionType)
        else [ann]
    )
    for m in candidates:
        if typing.get_origin(m) is typing.Annotated:
            for meta in typing.get_args(m)[1:]:
                pat = getattr(meta, "pattern", None)
                if isinstance(pat, str):
                    return pat
    return None


def _envelope(cls: type, spec: dict) -> CommentedMap:
    env = CommentedMap()
    env["kind"] = cls.__kind__
    env["apiVersion"] = cls.__api_version__
    env["type"] = qualified_class(cls)
    env["spec"] = spec
    return env


def _scalar_placeholder(members: list[Any], default: Any, path: str) -> Any:
    if default is not None:
        return default
    for b in members:
        origin = typing.get_origin(b)
        if origin is typing.Literal:
            args = typing.get_args(b)
            if args:
                return args[0]
        if isinstance(b, type) and issubclass(b, _enum.Enum):
            for member in b:
                return member.value
        if origin in (list, tuple, set, frozenset) or b in (list, tuple, set):
            return []
        if origin is dict or b is dict:
            return {}
        if b is bool:
            return False
        if b is int:
            return 0
        if b is float:
            return 0.0
    return f"<FILL: {path}>"


# ---------------------------------------------------------------------------
# Static template.
# ---------------------------------------------------------------------------


# A spec's keys sit two columns in from its ``spec:`` header; each nested
# envelope (``kind``/``apiVersion``/``type``/``spec``) adds another four.
_SPEC_INDENT = 2
_NEST_INDENT = 4


def _set_template_header(env: CommentedMap, conn_cls: type) -> None:
    """Prepend a header naming the connector / direction the template targets and
    reminding that secret values carry the ``secret:`` prefix (or a
    ``$secret:<ENV_VAR>`` environment reference resolved client-side at
    create/update time)."""
    label = conn_cls.__kind__
    for r in list_connectors():
        if r.conn_cls is conn_cls:
            label = f"{r.type} ({r.direction})"
            break
    env.yaml_set_start_comment(
        f"Connection template for: {label}\n"
        "Secret values use the 'secret:' prefix, or '$secret:<ENV_VAR>' to "
        "read the value from an environment variable.\n"
        "A '$secret:' value is resolved on the client when the "
        "source/destination is created or updated with this connection."
    )


def build_template(conn_cls: type, *, all_fields: bool = False) -> CommentedMap:
    """A fill-in connection envelope (a YAML-ready :class:`CommentedMap`).
    Required fields by default; optional fields too under ``all_fields``.
    Placeholders (``<FILL: ...>``) are not guaranteed to satisfy field regexes
    -- see the module / wizard split."""
    env = _envelope(conn_cls, _build_spec(conn_cls, "", all_fields, _SPEC_INDENT))
    _set_template_header(env, conn_cls)
    return env


def _is_secret(m: Any) -> bool:
    return isinstance(m, type) and issubclass(m, Secret)


def _placeholder_value(members: list[Any], default: Any, path: str) -> Any:
    """The fill-in placeholder a scalar (non-nested) field renders as.

    Secret-capable fields render a quoted ``<value>`` hint so the file shows
    exactly what to type; plain typed fields (Literal / enum / int / bool /
    list / dict, or a field carrying a default) keep a concrete valid sample.
    The :class:`SingleQuotedScalarString` wrapper both forces single-quote
    YAML rendering and marks the value as a fill-in placeholder, so
    :func:`_field_comment` knows to annotate it."""
    if default is not None:
        return default
    if any(_is_secret(m) for m in members):
        if any(not _is_secret(m) for m in members):
            return SingleQuotedScalarString("<value> or secret:<value>")
        return SingleQuotedScalarString("secret:<value>")
    if members[0] is str:
        return SingleQuotedScalarString("<value>")
    return _scalar_placeholder([members[0]], default, path)


def _expected_note(members: list[Any]) -> Optional[str]:
    """A very short 'what's expected' note for a union / dict / list / choice
    field — shown on optional fields and commented-out ones. ``None`` for a
    plain single-form scalar."""
    accepts = _type_comment(members)
    if accepts is not None:
        return accepts
    choices: list[str] = []
    for m in members:
        if typing.get_origin(m) is typing.Literal:
            choices.extend(str(a) for a in typing.get_args(m))
    if choices:
        return "one of: " + " | ".join(choices)
    for m in members:
        origin = typing.get_origin(m)
        if origin is dict or m is dict:
            return "a dict (str-keyed)"
        if origin in (list, tuple, set, frozenset) or m in (list, tuple, set):
            return "a list"
    return None


def _field_comment_lines(
    name: str, members: list[Any], value: Any, required: bool, default: Any
) -> Optional[list[str]]:
    """The content lines of the comment block shown above a real (uncommented)
    field (borders are added by :func:`_set_block_comment`). A required fill-in
    placeholder gets::

        # accepts: <value> | secret:<value>   (only when >1 accepted form)
        # replace <value> with the actual <name>

    an optional field shown with its default gets an ``optional (default: …)``
    note plus a short type note. ``None`` for a required field rendering a bare
    concrete sample."""
    if isinstance(value, SingleQuotedScalarString):
        lines = []
        accepts = _type_comment(members)
        if accepts is not None:
            lines.append(accepts)
        lines.append(f"replace <value> with the actual {name}")
        return lines
    if not required:
        lines = [f"optional (default: {default!r})"]
        note = _expected_note(members)
        if note is not None:
            lines.append(note)
        return lines
    return None


def _set_block_comment(
    cm: CommentedMap, key: str, lines: list[str], indent: int
) -> None:
    """Attach a comment block before ``key``: a blank-line separator, the
    given comment lines (each ``# <text>``), then a single ``#`` border just
    above the key. Built from low-level ruamel tokens because the high-level
    API renders empty lines as blank rather than as a bare ``#`` border."""
    mark = CommentMark(indent)
    tokens = [CommentToken("\n", mark)]
    tokens += [CommentToken(f"# {line}\n", mark) for line in lines]
    tokens.append(CommentToken("#\n", mark))
    cm.ca.items[key] = [None, tokens, None, None]


def _strip_comments(node: Any) -> Any:
    """Recursively drop attached comments, so a commented-out nested field's
    sample renders as plain structure (not comments-within-comments)."""
    if isinstance(node, CommentedMap):
        node.ca.items.clear()
        for v in node.values():
            _strip_comments(v)
    return node


def _commented_lines(
    name: str, first: Any, members: list[Any], indent: int
) -> list[str]:
    """Content lines (no leading ``#``) for an optional field whose default is
    None, rendered commented-out: a short note, then the field as it would be
    written (a scalar sample, or the full nested envelope)."""
    note = _expected_note(members)
    head = f"{name}  (optional, default: none)" + (f" -- {note}" if note else "")
    body_map = CommentedMap()
    if isinstance(first, type) and issubclass(first, DescriptorBase):
        obj = _first_concrete(first)
        body_map[name] = _strip_comments(
            _envelope(obj, _build_spec(obj, "", True, _SPEC_INDENT))
        )
    else:
        body_map[name] = _placeholder_value(members, None, name)
    return [head, *to_yaml(body_map).rstrip("\n").split("\n")]


def _deepest_last(cm: CommentedMap) -> tuple[CommentedMap, str]:
    """The owner map + key of the spec's last-rendered scalar leaf (drilling
    into the last nested map), where a trailing map comment must be attached."""
    key = next(reversed(cm))
    val = cm[key]
    if isinstance(val, CommentedMap) and len(val):
        return _deepest_last(val)
    return cm, key


def _attach_commented(out: CommentedMap, lines: list[str], indent: int) -> None:
    """Attach commented-out optional-field `lines` at the end of spec `out`
    (after its last real field), indented at the spec's own level. An
    all-optional spec (no real fields) gets them as a leading comment."""
    if not out:
        out.yaml_set_start_comment("\n".join(lines), indent=indent)
        return
    pad = " " * indent
    text = "\n" + "".join(f"{pad}# {ln}\n" if ln else f"{pad}#\n" for ln in lines)
    owner, key = _deepest_last(out)
    # Preserve any before-comment already on this leaf; only set its EOL slot.
    entry = owner.ca.items.get(key) or [None, None, None, None]
    entry[2] = CommentToken(text, CommentMark(indent))
    owner.ca.items[key] = entry


def _build_spec(cls: type, prefix: str, all_fields: bool, indent: int) -> CommentedMap:
    out = CommentedMap()
    commented: list[str] = []
    for name, fi in cls.__pydantic_fields__.items():
        required = fi.is_required()
        if not required and not all_fields:
            continue
        path = f"{prefix}{name}"
        members = _members(fi.annotation)
        # The sample is generated from the FIRST member of the field's union.
        first = members[0]
        default = None if fi.default is _UNSET else fi.default
        if not required and fi.default is None:
            # Optional, defaults to None -> shown commented-out (keeps the
            # rendered document valid) with a short 'what's expected' note.
            commented.extend(_commented_lines(name, first, members, indent))
            continue
        if isinstance(first, type) and issubclass(first, DescriptorBase):
            # Nested object -> its own envelope; no fill-in comment of its own.
            obj = _first_concrete(first)
            out[name] = _envelope(
                obj, _build_spec(obj, f"{path}.", all_fields, indent + _NEST_INDENT)
            )
            continue
        value = _placeholder_value(members, default, path)
        out[name] = value
        lines = _field_comment_lines(name, members, value, required, default)
        if lines is not None:
            _set_block_comment(out, name, lines, indent)
    if commented:
        _attach_commented(out, commented, indent)
    return out


# ---------------------------------------------------------------------------
# Interactive wizard.
# ---------------------------------------------------------------------------

# prompt(path, *, secret, optional, default, pattern, choices) -> str | None
PromptFn = Callable[..., Optional[str]]


def build_template_interactive(conn_cls: type, prompt: PromptFn) -> dict:
    """Walk every field, asking ``prompt`` per leaf, and assemble the envelope.
    Secret leaves capture an inline value only if the user supplies one (else a
    bare runtime-resolved :class:`~tabsdatak.api.Secret` ref); no value is ever
    required to be written to disk."""
    env = _envelope(conn_cls, _prompt_spec(conn_cls, "", prompt))
    _set_template_header(env, conn_cls)
    return env


def _prompt_spec(cls: type, prefix: str, prompt: PromptFn) -> dict:
    out: dict[str, Any] = {}
    for name, fi in cls.__pydantic_fields__.items():
        path = f"{prefix}{name}"
        optional = not fi.is_required()
        # Classify by the FIRST member of the field's union (mirrors the static
        # template).
        first = _members(fi.annotation)[0]
        ref = _ref_class(first)
        if isinstance(first, type) and issubclass(first, DescriptorBase):
            obj = _choose_concrete(first, path, prompt)
            out[name] = _envelope(obj, _prompt_spec(obj, f"{path}.", prompt))
        elif ref is not None:
            value = prompt(
                path,
                secret=True,
                optional=optional,
                default=None,
                pattern=None,
                choices=None,
            )
            # A user-supplied value is captured as the hydrated wire form
            # ("secret:<value>"), which `dehydrate` harvests out at upsert
            # time; with no value, the dehydrated marker (":secret:") is
            # written and the value is resolved at run time.
            out[name] = f"{ref._PREFIX}{value}" if value else ref().to_dict()
        else:
            value = prompt(
                path,
                secret=False,
                optional=optional,
                default=(None if fi.default is _UNSET else fi.default),
                pattern=_pattern_of(fi.annotation),
                choices=None,
            )
            if value is not None and value != "":
                out[name] = value
            elif not optional:
                out[name] = f"<FILL: {path}>"
    return out


def _choose_concrete(cls: type, path: str, prompt: PromptFn) -> type:
    """For a polymorphic nested type, ask which concrete subclass to use; for a
    single concrete/abstract type, resolve it directly."""
    subs = _concrete_subs(cls)
    if len(subs) <= 1:
        return _first_concrete(cls)
    names = [s.__name__ for s in subs]
    chosen = prompt(
        path,
        secret=False,
        optional=False,
        default=names[0],
        pattern=None,
        choices=names,
    )
    for s in subs:
        if s.__name__ == chosen:
            return s
    return subs[0]


# ---------------------------------------------------------------------------
# YAML serialization.
# ---------------------------------------------------------------------------


def _yaml() -> YAML:
    y = YAML()
    y.default_flow_style = False
    return y


def to_yaml(document: Any) -> str:
    """Render a connection document (the :class:`CommentedMap` from
    :func:`build_template` / :func:`build_template_interactive`, or any dict) as
    YAML text, including the field comments attached by the template engine."""
    buf = io.StringIO()
    _yaml().dump(document, buf)
    return buf.getvalue()


def from_yaml(text: str) -> Any:
    """Parse a YAML connection document into a plain Python structure. YAML is a
    superset of JSON, so a JSON document parses too."""
    return _yaml().load(text)


# ---------------------------------------------------------------------------
# Validation.
# ---------------------------------------------------------------------------


def validate(document: Any) -> str:
    """Validate a connection envelope by constructing the concrete connection
    via its own ``from_dict`` (full field + pattern validation, kind/type
    cross-check). Returns the ``kind``; raises :class:`TemplateError`."""
    if not isinstance(document, dict):
        raise TemplateError("connection document must be a JSON object")
    missing = _ENVELOPE_KEYS - document.keys()
    if missing:
        raise TemplateError(
            f"connection document missing envelope key(s): {', '.join(sorted(missing))}"
        )
    kind = document["kind"]
    if not isinstance(kind, str):
        raise TemplateError("connection document 'kind' must be a string")
    try:
        cls = resolve_class(document["type"])
    except ValueError as e:
        raise TemplateError(str(e))
    if not (isinstance(cls, type) and issubclass(cls, Conn)):
        raise TemplateError(
            f"type {document['type']!r} does not resolve to a connection class"
        )
    try:
        cls.from_dict(document)
    except Exception as e:  # noqa: BLE001 -- surface any construction failure
        raise TemplateError(f"invalid {kind} connection: {e}")
    return kind
