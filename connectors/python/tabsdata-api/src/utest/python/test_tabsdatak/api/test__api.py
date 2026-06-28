from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Annotated, Any, Union

import pytest
from pydantic import ValidationError

# noinspection PyProtectedMember
from tabsdatak import _api
from tabsdatak.api import (
    Var,
    Secret,
    Src,
)
from tabsdatak.error import (
    ApiErrorCode,
    ApiException,
    ApiInitException,
    DEBUG_FRAMES_ENV_VAR,
)
from tabsdatak import _info
from tabsdatak import api as m, tableframe
from tabsdatak import _connectors as _connectors
from tabsdatak import spi


# ---------------------------------------------------------------------------
# 1. ``_api.dataclass`` frozen-by-default invariant + opt-out
# ---------------------------------------------------------------------------


@_api.dataclass(ApiErrorCode.API_14)
class _FrozenByDefault(_api.JsonSerde):
    x: int


@_api.dataclass(ApiErrorCode.API_14, frozen=False)
class _MutableOptOut(_api.JsonSerde):
    x: int


def test_dataclass_is_frozen_by_default():
    obj = _FrozenByDefault(x=1)
    with pytest.raises((FrozenInstanceError, ValidationError, ApiException)):
        obj.x = 2  # type: ignore[misc]


def test_dataclass_frozen_false_opts_out():
    obj = _MutableOptOut(x=1)
    obj.x = 2  # type: ignore[misc]
    assert obj.x == 2


# ---------------------------------------------------------------------------
# 5. ``_api.dataclass`` error wrapping (ValidationError -> ApiException)
# ---------------------------------------------------------------------------


@_api.dataclass(ApiErrorCode.API_7)
class _RequiresField(_api.JsonSerde):
    name: str


def test_dataclass_wraps_validation_error_with_registered_exception(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")
    with pytest.raises(ApiException) as excinfo:
        _RequiresField()  # type: ignore[call-arg]
    assert excinfo.value.api_error is ApiErrorCode.API_7
    # API_7 declares ApiInitException as its exception class.
    assert type(excinfo.value) is ApiInitException
    # Cause is preserved as the underlying pydantic ValidationError.
    assert isinstance(excinfo.value.cause, ValidationError)


def test_dataclass_does_not_wrap_construction_errors_unrelated_to_validation():
    # A non-ValidationError raised during ``__init__`` is propagated
    # untouched (the wrapper only catches pydantic's ValidationError).
    @_api.dataclass(ApiErrorCode.API_14)
    class _Boomer(_api.JsonSerde):
        x: int

        def __post_init__(self):
            raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        _Boomer(x=1)


# ---------------------------------------------------------------------------
# 6. Polymorphic-field serialization: subclass at parent-typed slot
# ---------------------------------------------------------------------------


@_api.dataclass(ApiErrorCode.API_14)
class _SrcWithEnvField(Src):
    e: Var


def test_var_literal_at_field_dumps_to_string():
    # ``e: Var`` populated with an inline literal. A ``Var`` serializes to
    # the single ``"var:<value>"`` string -- not an envelope -- even when
    # it sits inside a polymorphic ``Src`` envelope.
    s = _SrcWithEnvField(e=Var("my-literal"))
    d = s.to_dict()
    assert d["spec"]["e"] == "var:my-literal"


def test_var_ref_at_field_dumps_to_bare_prefix():
    # An unhydrated ``Var()`` dumps to the bare ``"var:"`` placeholder.
    s = _SrcWithEnvField(e=Var())
    assert s.to_dict()["spec"]["e"] == "var:"


# ---------------------------------------------------------------------------
# 7. ``_JsonSerde`` instance pass-through
# ---------------------------------------------------------------------------


def test_already_instance_at_parent_slot_passes_through_unchanged():
    # Passing a pre-built Var instance as the value to a field must not
    # rebuild it -- the validator returns the existing instance.
    inner = Var("HOME")
    s = _SrcWithEnvField(e=inner)
    assert s.e is inner


def test_already_instance_via_pydantic_validator(monkeypatch):
    # The same identity is preserved when the instance flows through
    # pydantic dispatch (not just direct construction).
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")
    inner = Secret()
    # Round-trip: building a Src that holds the secret, then accessing
    # ``.e`` on a nested Var-typed field is the standard path; here we
    # just confirm the instance path through the schema's wrap-validator.
    @_api.dataclass(ApiErrorCode.API_14)
    class _SrcWithSecretField(Src):
        s: Secret

    src = _SrcWithSecretField(s=inner)
    assert src.s is inner

# ===========================================================================
# _api.JsonSerde / Info round-trips (was test_serde.py)
# ===========================================================================

class _SerdeSrcPlugin(spi.SrcPlugin): ...
class _SerdeDestPlugin(spi.DestPlugin): ...


@_api.dataclass(ApiErrorCode.API_14)
class _SerdeSrc(m.Src):
    name: str = "default"


_connectors.register_src(spi.SrcDef(
        conn=m.Conn,
    src_type="serde_test_src",
    src_version="v1",
    src=_SerdeSrc,
    table_mode=spi.TableMode.SINGLE,
    cardinality=lambda src: 1,
    plugin=_SerdeSrcPlugin,
    explorer=None,
))


@_api.dataclass(ApiErrorCode.API_14)
class _SerdeDest(m.Dest):
    name: str = "default"


_connectors.register_dest(spi.DestDef(
        conn=m.Conn,
    dest_type="serde_test_dest",
    dest_version="v1",
    dest=_SerdeDest,
    cardinality=lambda dest: 1,
    plugin=_SerdeDestPlugin,
    explorer=None,
))


def serde_pub_func(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
    return (t1,)


def serde_sub_func(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
    return (t1,)


def serde_xform_func(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
    return (t1,)


# Migration-chain fixtures must be at module top level so the type
# string ("module:qualname") resolves on load.

@_api.dataclass(ApiErrorCode.API_14)
class _MigratingSrc(m.Src):
    a: int
    b: int = 0
    c: int = 0


_MigratingSrc.__api_version__ = "1.2"
_MigratingSrc.__migrations__ = {
    "1.0": ("1.1", lambda s: {**s, "b": 10}),
    "1.1": ("1.2", lambda s: {**s, "c": 100}),
}


@_api.dataclass(ApiErrorCode.API_14)
class _MissingStepSrc(m.Src):
    a: int


_MissingStepSrc.__api_version__ = "2.0"
# No migration registered for "1.0" -> "2.0".


# ---------------------------------------------------------------------------
# Multi-style fixtures: a Conn subclass written each of the three ways.
# All must round-trip identically through _api.JsonSerde.
# ---------------------------------------------------------------------------

import dataclasses as _stdlib_dc
from pydantic import BaseModel
from pydantic.dataclasses import dataclass as _pdc


@_stdlib_dc.dataclass
class _StdlibConn(m.Conn):
    host: str
    port: int = 5432


@_pdc
class _PdcConn(m.Conn):
    host: str
    port: int = 5432


class _BmConn(m.Conn, BaseModel):
    host: str
    port: int = 5432


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_publisher() -> _info.PublisherInfo:
    @m.publisher(
        conn=m.NamedConn(name="conn1"),
        source=_SerdeSrc(name="src1"),
        output_tables=["out"],
    )
    def p(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)
    # Replace the local closure with a module-level function so the
    # serialization round-trip can resolve it. Decorators don't compose
    # neatly with module-level functions in test files, so we mutate
    # `func` post-hoc rather than re-architect the fixture.
    object.__setattr__(p, "func", serde_pub_func)
    return p


# ---------------------------------------------------------------------------
# _api.qualified_class / _api.resolve_class
# ---------------------------------------------------------------------------


def test_qualified_class_round_trips():
    s = _api.qualified_class(_SerdeSrc)
    assert _api.resolve_class(s) is _SerdeSrc


def test_qualified_class_rejects_local_class():
    class Local: pass
    with pytest.raises(ValueError, match="locally-defined"):
        _api.qualified_class(Local)


def test_resolve_class_rejects_bad_format():
    with pytest.raises(ValueError, match="expected"):
        _api.resolve_class("no_colon_here")


def test_resolve_class_rejects_unknown_module():
    with pytest.raises(ValueError, match="could not be imported"):
        _api.resolve_class("not_a_real_module_xyz:Foo")


def test_resolve_class_rejects_unknown_attribute():
    with pytest.raises(ValueError, match="attribute path not found"):
        _api.resolve_class("tabsdatak.api:DoesNotExist")


def test_resolve_class_rejects_non_class():
    with pytest.raises(ValueError, match="did not resolve to a class"):
        # `tabsdatak.api.publisher` is a function, not a class.
        _api.resolve_class("tabsdatak.api:publisher")


# ---------------------------------------------------------------------------
# _api.callable_to_str / _api.callable_from_str
# ---------------------------------------------------------------------------


def test_callable_round_trips():
    s = _api.callable_to_str(serde_pub_func)
    assert _api.callable_from_str(s) is serde_pub_func


def test_callable_to_str_rejects_closure():
    def local_fn(): pass
    with pytest.raises(ValueError, match="locally-defined"):
        _api.callable_to_str(local_fn)


# ---------------------------------------------------------------------------
# Plain _api.JsonSerde subclass round-trips
# ---------------------------------------------------------------------------


def test_named_conn_round_trips():
    nc = m.NamedConn(name="prod_db")
    d = nc.to_dict()
    assert d["kind"] == "connectionRef"
    assert d["type"].endswith(":NamedConn")
    assert d["spec"] == {"name": "prod_db"}
    assert m.NamedConn.from_dict(d) == nc


def test_env_round_trips():
    e = m.Var("DB_PASSWORD")
    d = e.to_dict()
    # A Var serializes to the single ``"var:<value>"`` string.
    assert d == "var:DB_PASSWORD"
    assert m.Var.from_dict(d) == e


def test_secret_ref_round_trips():
    s = m.Secret()
    d = s.to_dict()
    assert d == "secret:"
    assert m.Secret.from_dict(d) == s


def test_cron_trigger_round_trips():
    t = m.CronTrigger(mask="0 0 * * *")
    d = t.to_dict()
    assert d["kind"] == "trigger"
    assert d["type"].endswith(":CronTrigger")
    assert m.CronTrigger.from_dict(d) == t


def test_round_trip_via_json_string():
    nc = m.NamedConn(name="prod_db")
    s = nc.to_json()
    assert m.NamedConn.from_json(s) == nc


# ---------------------------------------------------------------------------
# Polymorphism: abstract base accepts envelope -> concrete instance
# ---------------------------------------------------------------------------


def test_conn_base_resolves_to_concrete_subclass():
    nc = m.NamedConn(name="prod")
    d = nc.to_dict()
    # Reconstruct via the base class -- it must dispatch to NamedConn.
    back = m.Conn.from_dict(d)
    assert isinstance(back, m.NamedConn)
    assert back == nc


def test_kind_mismatch_rejected():
    nc = m.NamedConn(name="prod")
    d = nc.to_dict()
    d["kind"] = m.KIND_SOURCE_DEF  # tampered
    with pytest.raises(Exception, match="kind"):
        m.Conn.from_dict(d)


def test_var_rejects_envelope_dict():
    # A Var only validates from a string (or a Var instance). Handing it a
    # NamedConn envelope dict is rejected.
    nc = m.NamedConn(name="prod")
    d = nc.to_dict()
    with pytest.raises(Exception, match="expects a string"):
        m.Var.from_dict(d)


# ---------------------------------------------------------------------------
# _info.PublisherInfo / _info.TransformerInfo / _info.SubscriberInfo round-trip
# ---------------------------------------------------------------------------


def test_publisher_round_trips_with_named_conn():
    src = _SerdeSrc(name="src1")
    p = _info.PublisherInfo(
        func=serde_pub_func,
        conn=m.NamedConn(name="prod_db"),
        source=src,
        output_tables=["out"],
        trigger_by=None,
        on_tables=None,
    )
    d = p.to_dict()
    assert d["kind"] == _info.KIND_PUBLISHER_DEF
    # Spec carries func as module:qualname.
    assert d["spec"]["func"].endswith(":serde_pub_func")
    # Conn is wrapped in its own envelope.
    assert d["spec"]["conn"]["kind"] == "connectionRef"
    assert d["spec"]["conn"]["spec"]["name"] == "prod_db"
    # Source is wrapped in its own envelope.
    assert d["spec"]["source"]["kind"] == m.KIND_SOURCE_DEF
    assert d["spec"]["source"]["spec"]["name"] == "src1"

    p2 = _info.PublisherInfo.from_dict(d)
    assert p2.func is serde_pub_func
    assert isinstance(p2.conn, m.NamedConn)
    assert p2.conn.name == "prod_db"
    assert isinstance(p2.source, _SerdeSrc)
    assert p2.source.name == "src1"
    assert p2.output_tables == ["out"]


def test_publisher_rejects_closure_func():
    p = _build_publisher()
    # Replace func with a closure post-construction to force the dump path.
    def closure(): ...
    object.__setattr__(p, "func", closure)
    with pytest.raises(Exception, match="locally-defined"):
        p.to_dict()


def test_publisher_round_trips_via_json_string():
    p = _info.PublisherInfo(
        func=serde_pub_func,
        conn=m.NamedConn(name="prod_db"),
        source=_SerdeSrc(name="src1"),
        output_tables=["out"],
        trigger_by=None,
        on_tables=None,
    )
    s = p.to_json()
    p2 = _info.PublisherInfo.from_json(s)
    assert p2.func is serde_pub_func
    assert p2.source.name == "src1"


def test_transformer_round_trips():
    t = _info.TransformerInfo(
        func=serde_xform_func,
        input_tables=["in"],
        output_tables=["out"],
        trigger_by=None,
        on_tables=None,
    )
    d = t.to_dict()
    assert d["kind"] == _info.KIND_TRANSFORMER_DEF
    t2 = _info.TransformerInfo.from_dict(d)
    assert t2.func is serde_xform_func
    assert t2.input_tables == ["in"]


def test_subscriber_round_trips():
    s = _info.SubscriberInfo(
        func=serde_sub_func,
        conn=m.NamedConn(name="dest_conn"),
        destination=_SerdeDest(name="dest1"),
        input_tables=["in"],
        trigger_by=None,
    )
    d = s.to_dict()
    assert d["kind"] == _info.KIND_SUBSCRIBER_DEF
    assert d["spec"]["destination"]["kind"] == m.KIND_DESTINATION_DEF
    s2 = _info.SubscriberInfo.from_dict(d)
    assert s2.func is serde_sub_func
    assert s2.destination.name == "dest1"


# ---------------------------------------------------------------------------
# Schema migration chain
# ---------------------------------------------------------------------------


def test_migration_chain_walked_to_current_version():
    # 1.0 spec ({"a": 1}) -> walk migrations -> 1.2 spec with all
    # three fields populated.
    envelope = {
        "kind": m.KIND_SOURCE_DEF,
        "apiVersion": "1.0",
        "type": _api.qualified_class(_MigratingSrc),
        "spec": {"a": 1},
    }
    obj = _MigratingSrc.from_dict(envelope)
    assert obj.a == 1
    assert obj.b == 10
    assert obj.c == 100


def test_migration_missing_step_raises():
    envelope = {
        "kind": m.KIND_SOURCE_DEF,
        "apiVersion": "1.0",
        "type": _api.qualified_class(_MissingStepSrc),
        "spec": {"a": 1},
    }
    with pytest.raises(Exception, match="no migration from version"):
        _MissingStepSrc.from_dict(envelope)


# A migration chain that loops 1.0 -> 1.1 -> 1.0 -- should be detected
# rather than walked forever.
@_api.dataclass(ApiErrorCode.API_14)
class _CycleSrc(m.Src):
    a: int


_CycleSrc.__api_version__ = "9.9"   # never reachable
_CycleSrc.__migrations__ = {
    "1.0": ("1.1", lambda s: s),
    "1.1": ("1.0", lambda s: s),
}


def test_migration_cycle_detected():
    envelope = {
        "kind": m.KIND_SOURCE_DEF,
        "apiVersion": "1.0",
        "type": _api.qualified_class(_CycleSrc),
        "spec": {"a": 1},
    }
    with pytest.raises(Exception, match="migration cycle"):
        _CycleSrc.from_dict(envelope)


# ---------------------------------------------------------------------------
# Malformed envelopes (missing required envelope keys) -- the wrap
# validator's structural check (`_ENVELOPE_KEYS <= value.keys()`) must
# reject them rather than fall through to the default validator.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("missing_key", ["kind", "apiVersion", "type", "spec"])
def test_envelope_missing_key_rejected(missing_key):
    nc = m.NamedConn(name="prod_db")
    envelope = nc.to_dict()
    envelope.pop(missing_key)
    with pytest.raises(Exception):
        m.NamedConn.from_dict(envelope)


# ---------------------------------------------------------------------------
# OnTablesAction list serde: a publisher's `on_tables` is a list of
# polymorphic _api.JsonSerde subclasses. Round-trip must preserve concrete
# types in the list.
# ---------------------------------------------------------------------------


@_api.dataclass(ApiErrorCode.API_17)
class _MarkAction(m.OnTablesAction):
    label: str


def test_on_tables_action_round_trips_inside_publisher():
    p = _info.PublisherInfo(
        func=serde_pub_func,
        conn=m.NamedConn(name="prod_db"),
        source=_SerdeSrc(name="src1"),
        output_tables=["out"],
        trigger_by=None,
        on_tables=[_MarkAction(label="hello")],
    )
    back = _info.PublisherInfo.from_json(p.to_json())
    assert isinstance(back.on_tables[0], _MarkAction)
    assert back.on_tables[0].label == "hello"


def test_on_tables_action_polymorphic_load_via_base():
    a = _MarkAction(label="hi")
    back = m.OnTablesAction.from_json(a.to_json())
    assert isinstance(back, _MarkAction)
    assert back.label == "hi"


# ---------------------------------------------------------------------------
# Multi-style: connector authors can use stdlib @_api.dataclass / pydantic
# _api.dataclass / BaseModel interchangeably.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("conn_cls", [_StdlibConn, _PdcConn, _BmConn])
def test_conn_round_trip_for_each_authoring_style(conn_cls):
    c = conn_cls(host="db.example", port=6543)
    s = c.to_json()
    back = m.Conn.from_json(s)  # via the abstract base -- polymorphic dispatch
    assert isinstance(back, conn_cls)
    assert back.host == "db.example"
    assert back.port == 6543


# ---------------------------------------------------------------------------
# NamedConn name field is a literal string only.
# ---------------------------------------------------------------------------


def test_named_conn_with_literal_name_round_trips():
    nc = m.NamedConn(name="MY_CONN_NAME")
    back = m.NamedConn.from_json(nc.to_json())
    assert back.name == "MY_CONN_NAME"


def test_named_conn_rejects_invalid_name():
    # Leading-digit identifier is not a valid named-connection name.
    with pytest.raises(ApiException) as excinfo:
        m.NamedConn(name="1bad")
    assert excinfo.value.api_error is ApiErrorCode.API_13


# ---------------------------------------------------------------------------
# Var / Secret constructor: ``X()`` is an unhydrated ref, ``X("v")`` an
# inline literal. Reading ``.value`` on a ref raises.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value", ["DB_PASSWORD", "_FOO", "x", "MIXED_case_42"])
def test_var_literal_holds_value(value):
    e = m.Var(value)
    assert e.value == value


def test_var_ref_value_raises():
    with pytest.raises(ApiException) as excinfo:
        m.Var().value
    assert excinfo.value.api_error is ApiErrorCode.API_8


def test_secret_ref_value_raises():
    with pytest.raises(ApiException) as excinfo:
        m.Secret().value
    assert excinfo.value.api_error is ApiErrorCode.API_11


# ---------------------------------------------------------------------------
# Round-trip through the JSON string form.
# ---------------------------------------------------------------------------


def test_var_json_round_trips():
    e = m.Var("DB_PASSWORD")
    back = m.Var.from_json(e.to_json())
    assert isinstance(back, m.Var)
    assert back.value == "DB_PASSWORD"


def test_secret_json_round_trips():
    s = m.Secret()
    back = m.Secret.from_json(s.to_json())
    assert isinstance(back, m.Secret)
    assert back._raw is None


# ---------------------------------------------------------------------------
# Timezone-aware datetime requirement on Src / Dest / Conn / Trigger.
# ---------------------------------------------------------------------------


from datetime import datetime as _dt, timezone as _tz


@_api.dataclass(ApiErrorCode.API_14)
class _SrcWithDt(m.Src):
    when: _dt


def test_naive_datetime_rejected_on_construction():
    with pytest.raises(ApiException, match="timezone-aware"):
        _SrcWithDt(when=_dt(2026, 1, 1))


def test_tz_aware_datetime_accepted_on_construction():
    s = _SrcWithDt(when=_dt(2026, 1, 1, tzinfo=_tz.utc))
    assert s.when.tzinfo is _tz.utc


def test_naive_iso_string_rejected_after_coercion():
    # Pydantic coerces "2026-01-01T00:00:00" into a naive datetime, then
    # the model validator catches it. The error path runs *after* coercion.
    with pytest.raises(ApiException, match="timezone-aware"):
        _SrcWithDt(when="2026-01-01T00:00:00")


def test_tz_aware_iso_string_accepted_after_coercion():
    s = _SrcWithDt(when="2026-01-01T00:00:00+00:00")
    assert s.when.tzinfo is not None


def test_cron_trigger_round_trips():
    # CronTrigger has only ``mask`` today (start / end were removed
    # from Trigger when the schema was simplified).
    t = m.CronTrigger(mask="0 0 * * *")
    back = m.CronTrigger.from_json(t.to_json())
    assert back.mask == t.mask


# ---------------------------------------------------------------------------
# `validate` cross-field hook on descriptor subclasses.
# ---------------------------------------------------------------------------


@_api.dataclass(ApiErrorCode.API_13)
class _PortRequiresHostConn(m.Conn):
    host: str | None = None
    port: int | None = None

    def validate(self):
        if self.port is not None and self.host is None:
            raise ValueError("port set without host")


def testvalidate_hook_runs_on_construction():
    # Inconsistent state -> validate raises -> wrapped as
    # ApiException with the class's API tag.
    with pytest.raises(ApiException) as excinfo:
        _PortRequiresHostConn(port=5432)
    assert excinfo.value.api_error is ApiErrorCode.API_13
    assert "port set without host" in str(excinfo.value)


def testvalidate_hook_silent_when_invariants_hold():
    c = _PortRequiresHostConn(host="localhost", port=5432)
    assert c.host == "localhost"
    assert c.port == 5432


def testvalidate_hook_runs_on_from_json_reload():
    # The hook fires on every construction path, including the one
    # `from_json` uses internally.
    c = _PortRequiresHostConn(host="localhost", port=5432)
    envelope = c.to_dict()
    envelope["spec"]["host"] = None       # tamper -> invariant broken
    import json
    with pytest.raises(Exception, match="port set without host"):
        _PortRequiresHostConn.from_json(json.dumps(envelope))


def test_defaultvalidate_is_no_op():
    # NamedConn doesn't override validate; it should construct cleanly.
    nc = m.NamedConn(name="prod_db")
    assert nc.name == "prod_db"


def test_naive_datetime_rejected_on_load_from_json():
    # Round-trip: a tampered envelope carrying a naive ISO datetime is
    # rejected on `from_json`. The error surfaces as pydantic's
    # ValidationError (the `_api.dataclass` ApiError-wrapper only fires
    # at construction time, not on `TypeAdapter.validate_json`).
    s = _SrcWithDt(when=_dt(2026, 1, 1, tzinfo=_tz.utc))
    envelope = s.to_dict()
    envelope["spec"]["when"] = "2026-01-01T00:00:00"   # strip the offset
    import json
    with pytest.raises(Exception, match="timezone-aware"):
        _SrcWithDt.from_json(json.dumps(envelope))


# ---------------------------------------------------------------------------
# Version specs -- port of the version-spec tests in
# server/libraries/td-objects/src/parse.rs (regex acceptance only; the
# parsed Version/Versions values are server-side concerns).
# ---------------------------------------------------------------------------

FIXED_ID = "0123456789ABCDEFGHIJKLMNOP"


def test_parse_version():
    assert _api.VERSION_REGEX.match("HEAD")
    assert _api.VERSION_REGEX.match("HEAD^")
    assert _api.VERSION_REGEX.match("HEAD~1")
    assert not _api.VERSION_REGEX.match("HEAD~a")
    assert _api.VERSION_REGEX.match("INITIAL")
    assert _api.VERSION_REGEX.match("INITIAL^")
    assert _api.VERSION_REGEX.match("INITIAL~1")
    assert not _api.VERSION_REGEX.match("INITIAL~a")
    assert _api.VERSION_REGEX.match(FIXED_ID)
    # "A" * 26 satisfies the pattern; Rust rejects it in IdL::parse, a
    # semantic id check that exists only on the server side.
    assert _api.VERSION_REGEX.match("A" * 26)


def test_parse_versions():
    assert _api.VERSIONS_REGEX.search("HEAD")
    assert _api.VERSIONS_REGEX.search("HEAD^")
    assert _api.VERSIONS_REGEX.search("HEAD~1")
    assert _api.VERSIONS_REGEX.search("HEAD~1,HEAD")
    assert _api.VERSIONS_REGEX.search("HEAD~1..HEAD")

    assert _api.VERSIONS_REGEX.search("INITIAL")
    assert _api.VERSIONS_REGEX.search("INITIAL^")
    assert _api.VERSIONS_REGEX.search("INITIAL~1")
    assert _api.VERSIONS_REGEX.search("INITIAL~1,INITIAL")
    assert _api.VERSIONS_REGEX.search("INITIAL~1..INITIAL")

    assert _api.VERSIONS_REGEX.search("INITIAL~1,HEAD")
    assert _api.VERSIONS_REGEX.search("INITIAL~1..HEAD")
    assert _api.VERSIONS_REGEX.search("HEAD~1,INITIAL")
    assert _api.VERSIONS_REGEX.search("HEAD~1..INITIAL")

    assert _api.VERSIONS_REGEX.search(FIXED_ID)
    assert _api.VERSIONS_REGEX.search(f"{FIXED_ID},HEAD~2")
    assert _api.VERSIONS_REGEX.search(f"HEAD~2,{FIXED_ID}")
    assert _api.VERSIONS_REGEX.search(f"{FIXED_ID},INITIAL~2")
    assert _api.VERSIONS_REGEX.search(f"INITIAL~2,{FIXED_ID}")

    assert not _api.VERSIONS_REGEX.search("HEAD~a")


def test_parse_versioned_table_ref():
    invalid = [
        " abc",
        "abc ",
        "abc/abc ",
        " abc/abc",
        "abc/abc@",
        "@abc",
        "abc/abc@HEAD..HEAD,HEAD",
        "abc/abc@HEAD..HEAD..HEAD",
        "abc/abc@HEAD~",
        "abc/abc@HEAD,,HEAD",
    ]
    for value in invalid:
        with pytest.raises(ValueError):
            _api._validate_coll_table_versions_name(value)

    valid = [
        "abc",
        "xyz/abc",
        "xyz/abc@HEAD",
        "xyz/abc@HEAD^^",
        "xyz/abc@HEAD^,HEAD",
        "xyz/abc@HEAD^..HEAD",
        "xyz/abc@HEAD~1",
        "xyz/abc@HEAD~10,HEAD",
        "xyz/abc@HEAD~10..HEAD",
    ]
    for value in valid:
        assert _api._validate_coll_table_versions_name(value) == value


def test_parse_valid_versioned_table_refs():
    valid_tables = [
        "table",
        "collection/table",
        "table@HEAD",
        "collection/table@HEAD",
        "collection/table@HEAD^",
        "collection/table@HEAD~1",
        "collection/table@HEAD^^^^,HEAD^,HEAD",
        "collection/table@HEAD^^..HEAD",
        "collection/table@INITIAL",
        "table@INITIAL",
        "collection/table@INITIAL^",
        "collection/table@INITIAL~1",
        "collection/table@INITIAL^^^^,INITIAL^,INITIAL",
        "collection/table@INITIAL^^..INITIAL",
        "collection/table@HEAD^^^^,INITIAL^,INITIAL",
        "collection/table@INITIAL^^..HEAD",
        "collection/table@INITIAL^^^^,HEAD^,INITIAL",
        "collection/table@NEW",
        f"collection/table@{FIXED_ID}",
    ]
    for value in valid_tables:
        assert _api._validate_coll_table_versions_name(value) == value


def test_parse_invalid_table_refs():
    invalid_table_refs = [
        "/table",
        "table/",
        "/table/",
        "collection//",
        "collection//table",
        "collection//table/",
        "collection/table/table/",
        "table@head",
        "table@initial",
        "collection/table@HEAD-1",
        "collection/table@INITIAL-1",
        "collection/table@01234567890123456789012",
        # Valid versioned, invalid refs
        "table@HEAD",
        "collection/table@HEAD",
        "collection/table@HEAD^",
        "collection/table@HEAD~1",
        "collection/table@HEAD^^^^,HEAD^,HEAD",
        "collection/table@HEAD^^..HEAD",
        "table@INITIAL",
        "collection/table@INITIAL",
        "collection/table@INITIAL^",
        "collection/table@INITIAL~1",
        "collection/table@INITIAL^^^^,INITIAL^,INITIAL",
        "collection/table@INITIAL^^..INITIAL",
        "collection/table@NEW^",
        "collection/table@NEW..HEAD^",
    ]
    for value in invalid_table_refs:
        with pytest.raises(ValueError):
            _api._validate_coll_table_name(value)
