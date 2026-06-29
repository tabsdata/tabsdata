from __future__ import annotations

import pytest

from tabsdatak.api import CronTrigger
from tabsdatak.error import (
    ApiErrorCode,
    ApiException,
    ApiInternalException,
    ApiInitException,
    ApiRuntimeException,
    DEBUG_FRAMES_ENV_VAR,
    ErrorCode,
    ErrorDef,
    TabsdataException,
)


# ---------------------------------------------------------------------------
# 1. ``ErrorCodeMeta`` invariants
# ---------------------------------------------------------------------------


def test_metaclass_rejects_non_errordef_value():
    with pytest.raises(TypeError, match="must be an ErrorDef"):
        class _BadCode(ErrorCode):
            X = "not-an-errordef"


def test_metaclass_rejects_non_exception_class():
    with pytest.raises(TypeError, match="ErrorDef.exception must be"):
        class _BadCode(ErrorCode):
            # exception is not a TabsdataException subclass.
            X = ErrorDef(exception="not-a-class", message_template="x")  # type: ignore[arg-type]


def test_metaclass_rejects_non_tabsdata_exception_class():
    with pytest.raises(TypeError, match="ErrorDef.exception must be"):
        class _BadCode(ErrorCode):
            # RuntimeError isn't a TabsdataException subclass.
            X = ErrorDef(exception=RuntimeError, message_template="x")  # type: ignore[arg-type]


def test_metaclass_accepts_any_tabsdata_exception_subclass():
    class _RuntimeExc(TabsdataException):
        pass

    class _GoodCode(ErrorCode):
        X = ErrorDef(ApiException, "x")
        Y = ErrorDef(_RuntimeExc, "y")

    assert _GoodCode.X.value.exception is ApiException
    assert _GoodCode.Y.value.exception is _RuntimeExc


def test_errordef_distinct_instances_do_not_collapse_into_enum_aliases():
    # ``ErrorDef`` uses ``eq=False`` so two instances with the same
    # field values are NOT equal -- if they were, the Enum machinery
    # would collapse them into aliases and silently merge variants.
    class _GoodCode(ErrorCode):
        X = ErrorDef(ApiException, "same message")
        Y = ErrorDef(ApiException, "same message")

    assert _GoodCode.X is not _GoodCode.Y
    assert _GoodCode.X.name == "X"
    assert _GoodCode.Y.name == "Y"


# ---------------------------------------------------------------------------
# 2. ``ErrorCode.exception()``
# ---------------------------------------------------------------------------


def test_exception_picks_class_from_variant(monkeypatch):
    # Disable source capture so the message format is deterministic.
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")

    pt = ApiErrorCode.API_2.exception()
    assert type(pt) is ApiInitException

    val = ApiErrorCode.API_6.exception(name="X")
    assert type(val) is ApiRuntimeException

    intern = ApiErrorCode.API_11.exception(name="X")
    assert type(intern) is ApiInternalException


def test_exception_format_map_interpolates_kwargs(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")
    exc = ApiErrorCode.API_6.exception(name="MY_SECRET", value="V", pattern="P")
    assert "Secret MY_SECRET value" in str(exc)


def test_exception_safe_format_keeps_unknown_placeholders(monkeypatch):
    # Missing kwargs round-trip as ``{key}`` instead of raising
    # KeyError.
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")
    exc = ApiErrorCode.API_6.exception()  # name/value/pattern missing
    rendered = str(exc)
    assert "{name}" in rendered
    assert exc.extra == {}


def test_exception_chains_cause(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")
    cause = RuntimeError("boom")
    exc = ApiErrorCode.API_1.exception(cause=cause)
    assert exc.cause is cause
    assert exc.__cause__ is cause
    assert exc.__suppress_context__ is True


def test_exception_runtime_subclass_used_directly(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")

    class _Runtime(TabsdataException):
        pass

    class _Codes(ErrorCode):
        X = ErrorDef(_Runtime, "rt")

    exc = _Codes.X.exception()
    assert type(exc) is _Runtime


# ---------------------------------------------------------------------------
# 3. ``_render_cause`` (exercised through TabsdataException rendering)
# ---------------------------------------------------------------------------


def _render(cause: Exception, monkeypatch) -> str:
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")
    exc = ApiErrorCode.API_1.exception(cause=cause)
    return str(exc)


def test_render_cause_pydantic_style_errors(monkeypatch):
    class _PydLike(Exception):
        def errors(self):
            return [
                {"loc": ("user", "name"), "msg": "field required"},
                {"loc": ("password",), "msg": "string too short"},
            ]

        def __str__(self):
            return "fallback-not-used"

    rendered = _render(_PydLike(), monkeypatch)
    assert "user.name: field required" in rendered
    assert "password: string too short" in rendered
    assert "fallback-not-used" not in rendered


def test_render_cause_plain_exception_uses_str(monkeypatch):
    rendered = _render(RuntimeError("plain message"), monkeypatch)
    assert "plain message" in rendered


def test_render_cause_falls_back_when_errors_raises(monkeypatch):
    class _Bad(Exception):
        def errors(self):
            raise ValueError("internal")

        def __str__(self):
            return "fallback-string"

    rendered = _render(_Bad(), monkeypatch)
    assert "fallback-string" in rendered


def test_render_cause_falls_back_when_errors_returns_non_list(monkeypatch):
    class _Weird(Exception):
        def errors(self):
            return {"not": "a-list"}

        def __str__(self):
            return "fallback-string"

    rendered = _render(_Weird(), monkeypatch)
    assert "fallback-string" in rendered


def test_render_cause_falls_back_on_empty_list(monkeypatch):
    class _Empty(Exception):
        def errors(self):
            return []

        def __str__(self):
            return "fallback-string"

    rendered = _render(_Empty(), monkeypatch)
    assert "fallback-string" in rendered


def test_render_cause_falls_back_on_non_dict_entries(monkeypatch):
    class _Mixed(Exception):
        def errors(self):
            return ["not-a-dict"]

        def __str__(self):
            return "fallback-string"

    rendered = _render(_Mixed(), monkeypatch)
    assert "fallback-string" in rendered


def test_render_cause_handles_entry_without_loc(monkeypatch):
    class _NoLoc(Exception):
        def errors(self):
            return [{"msg": "bare message"}]

    rendered = _render(_NoLoc(), monkeypatch)
    assert "bare message" in rendered


# ---------------------------------------------------------------------------
# 4. ``ErrorCode.__str__``
# ---------------------------------------------------------------------------


def test_errorcode_str_replaces_last_underscore_with_hyphen():
    # ``API_20`` → ``API-20``. The replacement is the last underscore
    # only, so multi-segment names keep the leading underscores intact.
    # The enum class name is intentionally not part of the string form.
    assert str(ApiErrorCode.API_20) == "API-20"


def test_errorcode_str_only_replaces_last_underscore():
    class _Codes(ErrorCode):
        FOO_BAR_BAZ = ErrorDef(ApiException, "x")

    assert str(_Codes.FOO_BAR_BAZ) == "FOO_BAR-BAZ"


# ---------------------------------------------------------------------------
# 5. ``TabsdataException`` rendering with a cause
# ---------------------------------------------------------------------------


def test_tabsdata_exception_renders_with_cause_no_source(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")
    exc = ApiErrorCode.API_6.exception(
        name="N", value="V", pattern="P", cause=RuntimeError("oops")
    )
    rendered = str(exc)
    # Format: "<code>: <message>: <cause>"
    assert rendered.startswith("API_6: Secret N value")
    assert "oops" in rendered
    # Source is disabled, so no " at " suffix.
    assert " at " not in rendered


def test_tabsdata_exception_renders_with_cause_and_source(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "1")
    exc = ApiErrorCode.API_6.exception(
        name="N", value="V", pattern="P", cause=RuntimeError("oops")
    )
    rendered = str(exc)
    # Source-suffix format: "... at <file>:<line>: <line-content>".
    assert " at " in rendered
    assert exc.source is not None
    assert "oops" in rendered
    assert exc.source in rendered


def test_tabsdata_exception_no_message_no_cause_renders_just_code(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")

    class _Codes(ErrorCode):
        # Empty template -- exercises the "no message" branch.
        X = ErrorDef(ApiException, "")

    exc = _Codes.X.exception()
    assert str(exc) == "X"


# ---------------------------------------------------------------------------
# 6. Every ApiErrorCode variant is wired to an ApiException subclass
# ---------------------------------------------------------------------------


def test_api_errorcode_every_variant_constructs_an_api_exception(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")
    for code in ApiErrorCode:
        exc = code.exception()
        assert isinstance(exc, ApiException)
        assert type(exc) is code.value.exception


# ---------------------------------------------------------------------------
# 7. User-source-frame capture (``TabsdataException.source``)
# ---------------------------------------------------------------------------


@pytest.fixture
def env_unset(monkeypatch):
    monkeypatch.delenv(DEBUG_FRAMES_ENV_VAR, raising=False)


@pytest.fixture
def env_disabled(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")


@pytest.fixture
def env_enabled(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "1")


def test_source_is_captured_by_default(env_unset):
    with pytest.raises(ApiException) as excinfo:
        CronTrigger()
    assert excinfo.value.source is not None
    assert __file__ in excinfo.value.source
    assert "CronTrigger()" in excinfo.value.source


def test_source_appears_in_str_repr_by_default(env_unset):
    with pytest.raises(ApiException) as excinfo:
        CronTrigger()
    rendered = str(excinfo.value)
    assert " at " in rendered
    assert excinfo.value.source in rendered


def test_explicit_truthy_env_var_keeps_capture_on(env_enabled):
    with pytest.raises(ApiException) as excinfo:
        CronTrigger()
    assert excinfo.value.source is not None
    assert __file__ in excinfo.value.source


@pytest.mark.parametrize(
    "falsy", ["0", "false", "FALSE", "False", "no", "off", "", "  0  "],
)
def test_falsy_env_var_disables_capture(monkeypatch, falsy):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, falsy)
    with pytest.raises(ApiException) as excinfo:
        CronTrigger()
    assert excinfo.value.source is None
    assert " at " not in str(excinfo.value)


def test_capture_skips_framework_frames(env_unset):
    # Sanity: even though pydantic / tabsdatak frames sit between
    # the user call and the exception construction, the captured
    # source is the user's call site, not a framework frame.
    with pytest.raises(ApiException) as excinfo:
        CronTrigger()
    src = excinfo.value.source
    assert src is not None
    parts = src.split(":")[0].split("/")
    assert "tabsdatak" not in parts
    assert "pydantic" not in parts


def test_capture_survives_synthetic_frames(env_unset):
    # Triggering construction from inside ``exec`` produces a frame
    # whose filename is ``"<string>"``. The capture must skip that
    # frame (or any other synthetic frame) without crashing.
    captured: dict = {}
    exec(  # noqa: S102 -- intentional to exercise the synthetic-frame branch
        "from tabsdatak.api import CronTrigger\n"
        "from tabsdatak.error import ApiException\n"
        "try:\n"
        "    CronTrigger()\n"
        "except ApiException as e:\n"
        "    captured['exc'] = e\n",
        {"captured": captured},
    )
    exc = captured["exc"]
    # The framework must NOT crash on the synthetic frame; whether
    # it found a user frame above the exec wrapper or returned None
    # is acceptable. What matters is the synthetic filename never
    # appears as the captured source.
    assert exc.source is None or "<string>" not in exc.source


def test_direct_construction_also_captures(env_unset):
    e = TabsdataException(api_error=ApiErrorCode.API_1, message="hi")
    assert e.source is not None
    assert __file__ in e.source


# ---------------------------------------------------------------------------
# 8. Direct-construction via the bound exception class
# ---------------------------------------------------------------------------


def test_metaclass_backlinks_exception_class_to_enum_member():
    class _Codes(ErrorCode):
        X = ErrorDef(ApiInitException, "x")

    assert ApiInitException._api_error is _Codes.X


def test_direct_construction_renders_template_from_bound_code(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")

    class _BoomError(TabsdataException):
        pass

    class _Codes(ErrorCode):
        BOOM = ErrorDef(_BoomError, "value {value!r} hit pattern {pattern!r}")

    exc = _BoomError(value=42, pattern="^foo$")
    assert isinstance(exc, _BoomError)
    assert exc.api_error is _Codes.BOOM
    assert exc.extra == {"value": 42, "pattern": "^foo$"}
    rendered = str(exc)
    assert "BOOM" in rendered
    assert "value 42 hit pattern '^foo$'" in rendered


def test_direct_construction_chains_cause(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")

    class _BoomError(TabsdataException):
        pass

    class _Codes(ErrorCode):
        BOOM = ErrorDef(_BoomError, "boom {what}")

    cause = RuntimeError("inner")
    exc = _BoomError(what="things", cause=cause)
    assert exc.cause is cause
    assert exc.__cause__ is cause
    assert exc.__suppress_context__ is True
    assert "inner" in str(exc)


def test_direct_construction_unknown_kwargs_left_as_placeholders(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")

    class _BoomError(TabsdataException):
        pass

    class _Codes(ErrorCode):
        BOOM = ErrorDef(_BoomError, "want {present} and {missing}")

    exc = _BoomError(present="here")
    rendered = str(exc)
    assert "here" in rendered
    assert "{missing}" in rendered
    assert exc.extra == {"present": "here"}


def test_direct_construction_without_binding_raises_typeerror():
    class _Orphan(TabsdataException):
        pass

    # No ErrorDef references _Orphan, so _api_error is never set.
    with pytest.raises(TypeError, match="not bound to an ErrorCode"):
        _Orphan(some="kwarg")


def test_direct_construction_with_explicit_api_error_overrides_binding(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")

    class _BoomError(TabsdataException):
        pass

    class _Codes(ErrorCode):
        BOOM = ErrorDef(_BoomError, "default-template {x}")
        OTHER = ErrorDef(_BoomError, "alt-template")

    exc = _BoomError(api_error=_Codes.OTHER, message="custom")
    assert exc.api_error is _Codes.OTHER
    assert "custom" in str(exc)
    assert "default-template" not in str(exc)


def test_factory_and_direct_paths_produce_equivalent_exceptions(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")

    class _BoomError(TabsdataException):
        pass

    class _Codes(ErrorCode):
        BOOM = ErrorDef(_BoomError, "value {value}")

    from_factory = _Codes.BOOM.exception(value=42)
    from_direct = _BoomError(value=42)
    assert type(from_factory) is type(from_direct)
    assert from_factory.api_error is from_direct.api_error
    assert from_factory.extra == from_direct.extra
    # Strings differ only by the captured source frame; with source
    # disabled the rendered messages are identical.
    assert str(from_factory) == str(from_direct)


def test_isinstance_with_bound_subclass_after_factory(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")

    class _BoomError(TabsdataException):
        pass

    class _Codes(ErrorCode):
        BOOM = ErrorDef(_BoomError, "x")

    exc = _Codes.BOOM.exception()
    assert isinstance(exc, _BoomError)
    assert type(exc) is _BoomError
