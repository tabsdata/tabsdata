from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Annotated, Any, Union

import pytest
from pydantic import ValidationError

# noinspection PyProtectedMember
from tabsdatak import _api
from tabsdatak.api import (
    Env,
    Secret,
    Src,
    LiteralEnv,
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
    e: Env


def test_subclass_instance_at_parent_field_dumps_subclass_envelope():
    # ``e: Env`` slot, but populated with a ``LiteralEnv`` (a subclass).
    # The wrap-serializer must re-route through the subclass schema so
    # the dump carries ``kind="literalEnv"`` (not the parent's
    # ``"envRef"``) and the subclass-only field ``s`` is present.
    s = _SrcWithEnvField(e=LiteralEnv(s="my-literal"))
    d = s.to_dict()
    inner = d["spec"]["e"]
    assert inner["kind"] == "literalEnv"
    assert inner["type"].endswith(":LiteralEnv")
    assert inner["spec"]["s"] == "my-literal"


# ---------------------------------------------------------------------------
# 7. ``_JsonSerde`` instance pass-through
# ---------------------------------------------------------------------------


def test_already_instance_at_parent_slot_passes_through_unchanged():
    # Constructing an Env instance, then passing it as the value to a
    # nested field, must not rebuild it -- the wrap-validator returns
    # the existing instance.
    inner = Env(s="HOME")
    s = _SrcWithEnvField(e=inner)
    assert s.e is inner


def test_already_instance_via_pydantic_validator(monkeypatch):
    # The same identity is preserved when the instance flows through
    # pydantic dispatch (not just direct construction).
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")
    inner = Secret(s="vault#PWD")
    # Round-trip: building a Src that holds the secret, then accessing
    # ``.e`` on a nested Env-typed field is the standard path; here we
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
    @m.publisher(source=_SerdeSrc(name="src1"), output_tables=["out"])
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
    e = m.Env("DB_PASSWORD")
    d = e.to_dict()
    assert d["kind"] == "envRef"
    assert m.Env.from_dict(d) == e


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
    d["kind"] = "sourceDef"  # tampered
    with pytest.raises(Exception, match="kind"):
        m.Conn.from_dict(d)


def test_subclass_check_rejects_unrelated_class():
    # Type points at NamedConn, but we're loading via Env.from_dict
    # (different hierarchy). Must reject.
    nc = m.NamedConn(name="prod")
    d = nc.to_dict()
    with pytest.raises(Exception, match="not a subclass"):
        m.Env.from_dict(d)


# ---------------------------------------------------------------------------
# _info.PublisherInfo / _info.TransformerInfo / _info.SubscriberInfo round-trip
# ---------------------------------------------------------------------------


def test_publisher_round_trips_with_named_conn():
    src = _SerdeSrc(name="src1")
    p = _info.PublisherInfo(
        func=serde_pub_func,
        source=src,
        output_tables=["out"],
        trigger_by=None,
        on_tables=None,
    )
    d = p.to_dict()
    assert d["kind"] == "publisherDef"
    # Spec carries func as module:qualname.
    assert d["spec"]["func"].endswith(":serde_pub_func")
    # Source is wrapped in its own envelope.
    assert d["spec"]["source"]["kind"] == "sourceDef"
    assert d["spec"]["source"]["spec"]["name"] == "src1"

    p2 = _info.PublisherInfo.from_dict(d)
    assert p2.func is serde_pub_func
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
    assert d["kind"] == "transformerDef"
    t2 = _info.TransformerInfo.from_dict(d)
    assert t2.func is serde_xform_func
    assert t2.input_tables == ["in"]


def test_subscriber_round_trips():
    s = _info.SubscriberInfo(
        func=serde_sub_func,
        destination=_SerdeDest(name="dest1"),
        input_tables=["in"],
        trigger_by=None,
    )
    d = s.to_dict()
    assert d["kind"] == "subscriberDef"
    assert d["spec"]["destination"]["kind"] == "destinationDef"
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
        "kind": "sourceDef",
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
        "kind": "sourceDef",
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
        "kind": "sourceDef",
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
# NamedConn name field accepts either a literal string or an Env reference
# ---------------------------------------------------------------------------


def test_named_conn_with_env_name_round_trips():
    nc = m.NamedConn(name=m.Env("MY_CONN_NAME"))
    back = m.NamedConn.from_json(nc.to_json())
    assert isinstance(back.name, m.Env)
    assert back.name.s == "MY_CONN_NAME"


def test_named_conn_with_literal_name_stillvalidates():
    # Leading-digit identifier still rejected -- both union arms fail
    # ("1bad" is not a valid named connection identifier and is not a
    # POSIX env-var name either). The pattern validator on the Env arm
    # surfaces API_6 directly.
    with pytest.raises(ApiException) as excinfo:
        m.NamedConn(name="1bad")
    assert excinfo.value.api_error in (ApiErrorCode.API_13, ApiErrorCode.API_6)


# ---------------------------------------------------------------------------
# Env name validator (POSIX-style env var name)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["DB_PASSWORD", "_FOO", "x", "MIXED_case_42"])
def test_env_accepts_valid_env_var_names(name):
    e = m.Env(name)
    assert e.s == name


@pytest.mark.parametrize("name", [
    "1starts_with_digit",
    "has-hyphen",
    "has space",
    "has.dot",
    "",
])
def test_env_rejects_invalid_env_var_names(name):
    with pytest.raises(ApiException) as excinfo:
        m.Env(name)
    assert excinfo.value.api_error is ApiErrorCode.API_7


# ---------------------------------------------------------------------------
# Secret reference validator (`<secret>#<key>`)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ref", [
    "db-password#DB_PASSWORD",   # canonical example
    "x#y",                        # minimal
    "ns.subdomain#KEY",           # multi-label DNS subdomain
    "abc123#key.with.dots",
    "abc#key-with-hyphens",
    "abc#key_with_underscores",
])
def test_secret_key_accepts_valid_refs(ref):
    s = m.Secret(ref)
    assert s.s == ref


@pytest.mark.parametrize("ref,reason", [
    ("no_separator",               "missing #"),
    ("UPPERCASE_SECRET#KEY",       "uppercase in secret name"),
    ("-leading-hyphen#KEY",        "secret label starts with hyphen"),
    ("trailing-hyphen-#KEY",       "secret label ends with hyphen"),
    ("secret#key with space",      "space in key"),
    ("secret#",                    "empty key"),
    ("#KEY",                       "empty secret name"),
])
def test_secret_key_rejects_invalid_refs(ref, reason):
    with pytest.raises(ApiException) as excinfo:
        m.Secret(ref)
    assert excinfo.value.api_error is ApiErrorCode.API_10


def test_secret_key_rejects_secret_name_over_253_chars():
    long_secret = "a" * 254
    with pytest.raises(ApiException):
        m.Secret(f"{long_secret}#KEY")


# ---------------------------------------------------------------------------
# Polymorphic dispatch through the value-ref family: loading via a
# concrete class round-trips, loading an unrelated envelope fails the
# issubclass check. (The shared abstract base ``ValOrRef`` lives in
# :mod:`tabsdatak._types` and is framework-internal; tests here use the
# public ``Env`` / ``Secret`` entry points instead.)
# ---------------------------------------------------------------------------


def test_env_polymorphic_dispatch_round_trips():
    e = m.Env("DB_PASSWORD")
    back = m.Env.from_json(e.to_json())
    assert isinstance(back, m.Env)
    assert back.s == "DB_PASSWORD"


def test_secret_polymorphic_dispatch_round_trips():
    s = m.Secret("db-password#DB_PASSWORD")
    back = m.Secret.from_json(s.to_json())
    assert isinstance(back, m.Secret)
    assert back.s == "db-password#DB_PASSWORD"


def test_env_rejects_unrelated_envelope():
    # Loading a NamedConn envelope via Env must fail the issubclass check.
    nc = m.NamedConn(name="prod_db")
    with pytest.raises(Exception, match="not a subclass"):
        m.Env.from_json(nc.to_json())


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
