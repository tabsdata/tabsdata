#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    CommandRuntimeException,
)
from tabsdatak.server._tdrunner.function._lib.request import (
    Info,
    InputTable,
    InvalidRequestError,
    Location,
    OutputTable,
    Request,
    load_request,
    load_request_or_raise,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


RESOURCES = Path(__file__).parent / "resources"


def _info_payload(**overrides: Any) -> dict:
    """A complete, valid `info` payload with overridable fields."""
    base: dict[str, Any] = {
        "project_id": "P-1",
        "collection_id": 19,
        "collection": "my_collection",
        "function_version_id": "FV-1",
        "function_defined_on": 1780000000000,
        "function": "publish_people",
        "function_run_id": 60,
        "function_bundle": {
            "uri": "s3://bucket/bundle.tar.gz",
            "env_prefix": "AWS_",
        },
        "function_stage": {
            "uri": "file:///tmp/stage",
            "env_prefix": "ROOT",
        },
        "triggered_on": 1780000000100,
        "transaction_id": 42,
        "execution_id": 24,
        "execution_name": None,
        "function_data": {
            "uri": "file:///tmp/data",
            "env_prefix": "ROOT",
        },
        "scheduled_on": 1780000000200,
    }
    base.update(overrides)
    return base


def _input_table_payload(**overrides: Any) -> dict:
    """A complete, valid InputTable payload (variant tag included)."""
    base: dict[str, Any] = {
        "name": "td-initial-values",
        "collection_id": 19,
        "collection": "my_collection",
        "table_id": "T-1",
        "table_version_id": "TV-1",
        "execution_id": None,
        "transaction_id": None,
        "function_run_id": None,
        "triggered_on": None,
        "table_data_version_id": None,
        "location": None,
        "input_idx": 0,
        "table_pos": -1,
        "version_pos": -1,
    }
    base.update(overrides)
    return {"Table": base}


def _output_table_payload(**overrides: Any) -> dict:
    """A complete, valid OutputTable payload (variant tag included)."""
    base: dict[str, Any] = {
        "name": "customers",
        "collection_id": 19,
        "collection": "my_collection",
        "table_id": "T-2",
        "table_version_id": "TV-2",
        "table_data_version_id": "PDV-1",
        "location": {
            "uri": "s3://bucket/out/customers.parquet",
            "env_prefix": "AWS_",
        },
        "table_pos": 0,
    }
    base.update(overrides)
    return {"Table": base}


def _envelope(payload: dict) -> dict:
    return {"FunctionV2": payload}


def _write(tmp_path: Path, body: Any, name: str = "request.yaml") -> Path:
    p = tmp_path / name
    if isinstance(body, str):
        p.write_text(body)
    else:
        p.write_text(yaml.safe_dump(body))
    return p


def _request_payload(
    *,
    info: dict | None = None,
    input: list | None = None,
    output: list | None = None,
    system_input: list | None = None,
    system_output: list | None = None,
) -> dict:
    return _envelope({
        "info": info if info is not None else _info_payload(),
        "input": input if input is not None else [],
        "output": output if output is not None else [],
        "system_input": system_input if system_input is not None else [],
        "system_output": system_output if system_output is not None else [],
    })


# --- happy path ---


def test_load_minimal_fixture():
    req = load_request(RESOURCES / "minimal_publisher_request.yaml")
    assert isinstance(req, Request)
    assert req.info.collection == "my_collection"
    assert req.info.collection_id == 19
    assert req.info.execution_id == 24
    assert req.info.transaction_id == 42
    assert req.info.function_run_id == 60
    assert req.info.function_bundle == Location(
        uri="s3://bucket/bundle.tar.gz", env_prefix="AWS_"
    )
    assert req.input == ()
    assert len(req.output) == 1
    assert req.output[0].name == "customers"
    assert req.output[0].location.uri == "s3://bucket/out/customers.parquet"
    assert req.system_input[0].name == "td-initial-values"
    assert req.system_output[0].location.uri == "s3://bucket/state/out.parquet"


def test_load_string_ids(tmp_path):
    p = _write(
        tmp_path,
        _request_payload(
            info=_info_payload(
                collection_id="collection-uuid",
                execution_id="exec-uuid",
                transaction_id="tx-uuid",
                function_run_id="run-uuid",
                function_version_id="fv-uuid",
            ),
        ),
    )
    req = load_request(p)
    assert req.info.collection_id == "collection-uuid"
    assert req.info.execution_id == "exec-uuid"
    assert req.info.transaction_id == "tx-uuid"
    assert req.info.function_run_id == "run-uuid"


def test_load_bundle_without_env_prefix(tmp_path):
    p = _write(
        tmp_path,
        _request_payload(
            info=_info_payload(
                function_bundle={"uri": "file:///tmp/b.tgz"},
            ),
        ),
    )
    req = load_request(p)
    assert req.info.function_bundle.env_prefix is None


def test_load_bundle_with_explicit_null_env_prefix(tmp_path):
    p = _write(
        tmp_path,
        _request_payload(
            info=_info_payload(
                function_bundle={"uri": "file:///tmp/b.tgz", "env_prefix": None},
            ),
        ),
    )
    req = load_request(p)
    assert req.info.function_bundle.env_prefix is None


def test_load_input_table_without_location_key(tmp_path):
    """Input tables can omit `location` entirely (it's optional)."""
    p = _write(
        tmp_path,
        _request_payload(
            input=[
                {
                    "Table": {
                        "name": "bare_input",
                        "collection_id": 1,
                        "collection": "c",
                        "table_id": "T-x",
                        "table_version_id": "TV-x",
                        "input_idx": 0,
                        "table_pos": 0,
                        "version_pos": 0,
                    }
                }
            ],
        ),
    )
    req = load_request(p)
    assert req.input == (
        InputTable(
            name="bare_input",
            collection_id=1,
            collection="c",
            table_id="T-x",
            table_version_id="TV-x",
            input_idx=0,
            table_pos=0,
            version_pos=0,
        ),
    )


def test_load_input_table_with_explicit_null_location(tmp_path):
    p = _write(
        tmp_path,
        _request_payload(input=[_input_table_payload(location=None)]),
    )
    req = load_request(p)
    assert req.input[0].location is None


def test_load_omits_table_lists_default_to_empty(tmp_path):
    """If a top-level table list key is missing, treat as empty."""
    p = _write(tmp_path, _envelope({"info": _info_payload()}))
    req = load_request(p)
    assert req.input == ()
    assert req.output == ()
    assert req.system_input == ()
    assert req.system_output == ()


# --- envelope handling ---


def test_load_unknown_top_level_variant(tmp_path):
    """A variant tag not in REQUEST_ENVELOPE_KNOWN is reported as unknown."""
    p = _write(tmp_path, {"FunctionV3": {"info": _info_payload()}})
    with pytest.raises(
        InvalidRequestError,
        match=r"unknown variant 'FunctionV3'; expected one of: FunctionV2",
    ):
        load_request(p)


def test_load_top_level_multi_key_envelope(tmp_path):
    p = _write(tmp_path, {"FunctionV2": {}, "extra": 1})
    with pytest.raises(InvalidRequestError, match="single-key variant envelope"):
        load_request(p)


def test_load_known_but_unsupported_table_variant(tmp_path):
    """Variants Rust knows about (e.g. PartitionedTable) but we don't yet
    handle are reported as recognized-but-not-supported."""
    p = _write(
        tmp_path,
        _request_payload(input=[{"PartitionedTable": {"name": "x"}}]),
    )
    with pytest.raises(
        InvalidRequestError,
        match=r"variant 'PartitionedTable' is recognized but not yet supported",
    ):
        load_request(p)


def test_load_truly_unknown_table_variant(tmp_path):
    """Variants outside the known set are reported as unknown with the
    list of known variants."""
    p = _write(
        tmp_path,
        _request_payload(input=[{"NotARealVariant": {"name": "x"}}]),
    )
    with pytest.raises(
        InvalidRequestError,
        match=(
            r"unknown variant 'NotARealVariant'; expected one of: "
            r"Table, TableVersions, PartitionedTable, PartitionedTableVersions"
        ),
    ):
        load_request(p)


def test_load_table_variant_payload_not_a_mapping(tmp_path):
    p = _write(
        tmp_path,
        _request_payload(input=[{"Table": "scalar"}]),
    )
    with pytest.raises(InvalidRequestError, match="payload must be a mapping"):
        load_request(p)


# --- file / yaml errors ---


def test_load_missing_file_raises_filenotfound(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_request(tmp_path / "nope.yaml")


def test_load_malformed_yaml_raises_invalid_request(tmp_path):
    p = _write(tmp_path, "info: { unclosed")
    with pytest.raises(InvalidRequestError, match="malformed YAML"):
        load_request(p)


def test_load_top_level_not_a_mapping(tmp_path):
    p = _write(tmp_path, "- a\n- b\n")
    with pytest.raises(InvalidRequestError):
        load_request(p)


# --- missing / wrong required fields (validated by pydantic) ---


def test_load_missing_info(tmp_path):
    p = _write(tmp_path, _envelope({"input": []}))
    with pytest.raises(InvalidRequestError, match="info"):
        load_request(p)


def test_load_info_not_a_mapping(tmp_path):
    p = _write(tmp_path, _envelope({"info": "not-a-dict"}))
    with pytest.raises(InvalidRequestError, match="info"):
        load_request(p)


@pytest.mark.parametrize(
    "missing_field",
    [
        "collection",
        "collection_id",
        "execution_id",
        "transaction_id",
        "function_run_id",
        "function_bundle",
        "function_version_id",
        "function_defined_on",
        "function",
        "function_stage",
        "triggered_on",
        "function_data",
        "scheduled_on",
    ],
)
def test_load_missing_required_info_field(tmp_path, missing_field):
    info = _info_payload()
    info.pop(missing_field)
    p = _write(tmp_path, _envelope({"info": info}))
    with pytest.raises(InvalidRequestError) as exc_info:
        load_request(p)
    assert missing_field in str(exc_info.value)


def test_load_collection_wrong_type(tmp_path):
    p = _write(
        tmp_path,
        _envelope({"info": _info_payload(collection=12345)}),
    )
    with pytest.raises(InvalidRequestError, match="info.collection"):
        load_request(p)


def test_load_collection_id_wrong_type(tmp_path):
    p = _write(
        tmp_path,
        _envelope({"info": _info_payload(collection_id=[1, 2, 3])}),
    )
    with pytest.raises(InvalidRequestError, match="info.collection_id"):
        load_request(p)


def test_load_function_bundle_not_a_mapping(tmp_path):
    p = _write(
        tmp_path,
        _envelope({"info": _info_payload(function_bundle="just-a-string")}),
    )
    with pytest.raises(InvalidRequestError, match="function_bundle"):
        load_request(p)


def test_load_location_missing_uri(tmp_path):
    p = _write(
        tmp_path,
        _envelope({"info": _info_payload(function_bundle={"env_prefix": "AWS_"})}),
    )
    with pytest.raises(InvalidRequestError, match="uri"):
        load_request(p)


def test_load_location_uri_wrong_type(tmp_path):
    p = _write(
        tmp_path,
        _envelope({"info": _info_payload(function_bundle={"uri": 42})}),
    )
    with pytest.raises(InvalidRequestError, match="uri"):
        load_request(p)


def test_load_location_env_prefix_wrong_type(tmp_path):
    p = _write(
        tmp_path,
        _envelope({
            "info": _info_payload(
                function_bundle={"uri": "file:///b", "env_prefix": 42},
            ),
        }),
    )
    with pytest.raises(InvalidRequestError, match="env_prefix"):
        load_request(p)


def test_load_output_table_requires_location(tmp_path):
    """OutputTable has a non-optional location (unlike InputTable)."""
    payload = _output_table_payload()
    payload["Table"].pop("location")
    p = _write(tmp_path, _request_payload(output=[payload]))
    with pytest.raises(InvalidRequestError, match="location"):
        load_request(p)


def test_load_output_table_requires_table_data_version_id(tmp_path):
    payload = _output_table_payload()
    payload["Table"].pop("table_data_version_id")
    p = _write(tmp_path, _request_payload(output=[payload]))
    with pytest.raises(InvalidRequestError, match="table_data_version_id"):
        load_request(p)


# --- table-list errors ---


def test_load_table_list_not_a_list(tmp_path):
    p = _write(tmp_path, _request_payload(output="not-a-list"))
    with pytest.raises(InvalidRequestError, match="output"):
        load_request(p)


def test_load_table_entry_not_a_mapping(tmp_path):
    p = _write(tmp_path, _request_payload(output=["just-a-string"]))
    with pytest.raises(InvalidRequestError, match="output"):
        load_request(p)


def test_load_table_missing_name(tmp_path):
    payload = _output_table_payload()
    payload["Table"].pop("name")
    p = _write(tmp_path, _request_payload(output=[payload]))
    with pytest.raises(InvalidRequestError, match="name"):
        load_request(p)


def test_load_nested_table_location_uri_wrong_type(tmp_path):
    payload = _input_table_payload(
        location={"uri": 42},
    )
    p = _write(tmp_path, _request_payload(input=[payload]))
    with pytest.raises(InvalidRequestError, match="uri"):
        load_request(p)


# --- extra="forbid": unknown fields are rejected ---


def test_load_extra_field_at_top_level_is_forbidden(tmp_path):
    p = _write(
        tmp_path,
        _envelope({
            "info": _info_payload(),
            "unknown_top_level": "hi",
        }),
    )
    with pytest.raises(InvalidRequestError, match="unknown_top_level"):
        load_request(p)


def test_load_extra_field_in_info_is_forbidden(tmp_path):
    p = _write(
        tmp_path,
        _envelope({"info": _info_payload(surprise=1)}),
    )
    with pytest.raises(InvalidRequestError, match="surprise"):
        load_request(p)


# --- frozen ---


def test_request_is_frozen():
    req = load_request(RESOURCES / "minimal_publisher_request.yaml")
    with pytest.raises(Exception):
        req.input = ()  # type: ignore[misc]


def test_location_is_frozen():
    loc = Location(uri="s3://x")
    with pytest.raises(Exception):
        loc.uri = "other"  # type: ignore[misc]


def test_input_table_is_frozen():
    t = InputTable(
        name="x",
        collection_id=1,
        collection="c",
        table_id="T",
        table_version_id="TV",
        input_idx=0,
        table_pos=0,
        version_pos=0,
    )
    with pytest.raises(Exception):
        t.name = "other"  # type: ignore[misc]


def test_output_table_is_frozen():
    t = OutputTable(
        name="x",
        collection_id=1,
        collection="c",
        table_id="T",
        table_version_id="TV",
        table_data_version_id="PDV",
        location=Location(uri="s3://x"),
        table_pos=0,
    )
    with pytest.raises(Exception):
        t.name = "other"  # type: ignore[misc]


# --- Info derived timestamp properties (TD-2313) ----------------------------
#
# ``Info.triggered_on`` is epoch milliseconds on the wire. The derived
# helpers convert that to a tz-aware UTC datetime (consumed by
# ``stamp-inputs`` to populate ``$td.ver.timestamp``) and to an
# ISO-8601 string (consumed by ``FromRequestCtx.exec_plan_ts``, which
# satisfies the public ``TrxCtx.exec_plan_ts`` SPI contract). These
# tests pin the conversion shape so a future change to the wire format
# (e.g. epoch seconds instead of milliseconds) breaks loudly here
# rather than silently corrupting downstream column values.


def _build_info(triggered_on: int) -> Info:
    return Info(
        project_id="P-1",
        collection_id=19,
        collection="c",
        function_version_id="FV-1",
        function_defined_on=1780000000000,
        function="publish_people",
        function_run_id=60,
        function_bundle=Location(uri="s3://b/bundle"),
        function_stage=Location(uri="file:///tmp/stage"),
        triggered_on=triggered_on,
        transaction_id=42,
        execution_id=24,
        function_data=Location(uri="file:///tmp/data"),
        scheduled_on=1780000000200,
    )


def test_info_triggered_on_datetime_returns_utc_aware_datetime():
    info = _build_info(triggered_on=1780562068376)
    dt = info.triggered_on_datetime
    assert isinstance(dt, datetime)
    assert dt.tzinfo is timezone.utc
    # 1780562068376 ms == 2026-06-04 08:34:28.376 UTC
    assert dt.year == 2026
    assert dt.microsecond == 376000


def test_info_triggered_on_datetime_handles_zero_millis():
    """0 ms epoch maps to the Unix epoch itself."""
    info = _build_info(triggered_on=0)
    assert info.triggered_on_datetime == datetime(1970, 1, 1, tzinfo=timezone.utc)


def test_info_triggered_on_iso_returns_iso_string():
    info = _build_info(triggered_on=1780562068376)
    # Pinned exactly so a wire-format change (e.g. seconds vs millis,
    # or losing the tz suffix) fails loudly.
    assert info.triggered_on_iso == "2026-06-04T08:34:28.376000+00:00"


def test_info_triggered_on_iso_matches_datetime_isoformat():
    """The iso helper is just ``triggered_on_datetime.isoformat()``;
    pin the relationship so callers can rely on it."""
    info = _build_info(triggered_on=1780000000100)
    assert info.triggered_on_iso == info.triggered_on_datetime.isoformat()


# --- load_request_or_raise --------------------------------------------


class _ProbeError(CommandRuntimeException):
    """Stand-in per-command bad-request exception for the helper tests."""

    exit_code = 99


class _ProbeErrorCode(ErrorCode):
    PROBE_1 = ErrorDef(_ProbeError, "probe: {path} {detail}")


def test_load_request_or_raise_returns_request_on_happy_path():
    req = load_request_or_raise(
        RESOURCES / "minimal_publisher_request.yaml",
        _ProbeErrorCode.PROBE_1,
    )
    assert isinstance(req, Request)


def test_load_request_or_raise_wraps_file_not_found(tmp_path):
    missing = tmp_path / "nope.yaml"
    with pytest.raises(_ProbeError) as exc_info:
        load_request_or_raise(missing, _ProbeErrorCode.PROBE_1)
    assert exc_info.value.exit_code == 99
    assert isinstance(exc_info.value.__cause__, FileNotFoundError)
    # Both ``path`` and ``detail`` reach the rendered message.
    rendered = str(exc_info.value)
    assert str(missing) in rendered


def test_load_request_or_raise_wraps_malformed_yaml(tmp_path):
    bad = tmp_path / "request.yaml"
    bad.write_text("info: { unclosed")
    with pytest.raises(_ProbeError) as exc_info:
        load_request_or_raise(bad, _ProbeErrorCode.PROBE_1)
    assert exc_info.value.exit_code == 99
    assert isinstance(exc_info.value.__cause__, InvalidRequestError)


def test_load_request_or_raise_stores_path_and_detail_in_extra(tmp_path):
    missing = tmp_path / "nope.yaml"
    try:
        load_request_or_raise(missing, _ProbeErrorCode.PROBE_1)
    except _ProbeError as exc:
        assert exc.extra["path"] == missing
        assert "detail" in exc.extra
