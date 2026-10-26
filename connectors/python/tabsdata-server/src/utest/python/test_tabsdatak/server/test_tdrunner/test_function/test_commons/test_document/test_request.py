#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from tabsdatak.common._storage.mounts import (
    AnonymousMountDeclaration,
    EnvironmentMountDeclaration,
)
from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._commons.document.request import (
    AnyRequest,
    InputTable,
    InputTableVersions,
    Location,
    OutputTable,
    Request,
    StreamerRequest,
)
from tabsdatak.server._tdrunner.function._lib.errors import CommandRuntimeException

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

RESOURCES = Path(__file__).parent / "resources"
MINIMAL = RESOURCES / "minimal_publisher_request.yaml"


class RequestFailure(CommandRuntimeException):
    exit_code = 10


class RequestErrorCode(ErrorCode):
    READ = ErrorDef(RequestFailure, "Cannot read the request {path}: {detail}")


def info(**overrides: Any) -> dict:
    return {
        "project_id": "P-1",
        "project": "p",
        "collection_id": 19,
        "collection": "my_collection",
        "function_id": "F-1",
        "function": "publish_people",
        "function_version_id": "FV-1",
        "function_defined_on": 1780000000000,
        "function_bundle": {"uri": "s3://bucket/bundle.tar.zst", "env_prefix": "AWS_"},
        "function_stage": {"uri": "file:///tmp/stage", "env_prefix": "ROOT"},
        "execution_id": 24,
        "execution": None,
        "transaction_id": 42,
        "logical_function_run_id": 60,
        "physical_function_run_id": 60,
        "triggered_on": 1780000000100,
        "function_data": {"uri": "file:///tmp/data", "env_prefix": "ROOT"},
        "scheduled_on": 1780000000200,
        **overrides,
    }


def input_table(**overrides: Any) -> dict:
    return {
        "collection_id": 19,
        "collection": "my_collection",
        "table_id": "T-1",
        "table": "td-initial-values",
        "table_version_id": "TV-1",
        "input_idx": 0,
        "table_pos": -1,
        "version_pos": -1,
        **overrides,
    }


def output_table(**overrides: Any) -> dict:
    return {
        "collection_id": 19,
        "collection": "my_collection",
        "table_id": "T-2",
        "table": "customers",
        "table_version_id": "TV-2",
        "logical_table_data_version_id": "LDV-1",
        "physical_table_data_version_id": "PDV-1",
        "location": {"uri": "s3://bucket/out/customers.parquet", "env_prefix": "AWS_"},
        "table_pos": 0,
        **overrides,
    }


def function(**slots: Any) -> dict:
    return {"Function": {"info": info(), **slots}}


def written(tmp_path: Path, document: Any) -> Path:
    path = tmp_path / "request.yaml"
    path.write_text(document if isinstance(document, str) else yaml.safe_dump(document))
    return path


def load(path: Path) -> Request:
    return Request.load(path, RequestErrorCode.READ)


def failure(tmp_path: Path, document: Any) -> str:
    with pytest.raises(RequestFailure) as raised:
        load(written(tmp_path, document))
    return str(raised.value)


def test_load_reads_the_minimal_request():
    request = load(MINIMAL)
    assert request.info.function == "publish_people"
    assert request.info.function_bundle == Location(
        uri="s3://bucket/bundle.tar.zst",
        env_prefix="AWS_",
    )
    assert [table.table for table in request.system_input] == ["td-initial-values"]
    assert [table.table for table in request.output] == ["customers"]
    assert request.input == ()
    assert [table.table for table in request.system_output] == ["td-initial-values"]


def test_slots_follow_the_kubernetes_wire_order():
    assert list(Request.model_fields) == [
        "info",
        "system_input",
        "input",
        "system_output",
        "output",
    ]


def test_load_accepts_string_ids_and_missing_optionals(tmp_path: Path):
    document = function(
        input=[{"Table": input_table(collection_id="C-1", table_id="T-9")}],
    )
    document["Function"]["info"]["function_bundle"] = {"uri": "file:///bundle"}
    request = load(written(tmp_path, document))
    assert request.input[0].collection_id == "C-1"
    assert request.input[0].location is None
    assert request.info.function_bundle.env_prefix is None


def test_load_unwraps_table_versions(tmp_path: Path):
    versions = {
        "is_empty": False,
        "versions": [input_table(version_pos=0), input_table(version_pos=1)],
    }
    request = load(written(tmp_path, function(input=[{"TableVersions": versions}])))
    [entry] = request.input
    assert isinstance(entry, InputTableVersions)
    assert [table.version_pos for table in entry.tables] == [0, 1]


def test_single_table_lists_itself_as_its_tables():
    table = InputTable.model_validate(input_table())
    assert table.tables == (table,)


@pytest.mark.parametrize(
    "document, message",
    [
        (
            function(input=[{"PartitionedTable": {}}]),
            "recognized but not yet supported",
        ),
        (function(output=[{"TableVersions": {}}]), "recognized but not yet supported"),
        (function(input=[{"Mystery": {}}]), "is unknown; expected one of"),
        (function(input=[{"Table": input_table(), "Mystery": {}}]), "exactly one"),
        (function(input=["not a mapping"]), "exactly one variant"),
        (function(input={"Table": {}}), "must hold a list"),
        ({"Streamer": {"info": info()}}, "Function"),
        ({"Function": {"info": info()}, "Streamer": {}}, "Streamer"),
        ([1, 2], "does not match its model"),
        ("Function: { unclosed", "Cannot read the document"),
        (function(info=None), "info"),
        (function(output=[{"Table": output_table(location=None)}]), "location"),
        (function(input=[{"Table": input_table(table=None)}]), "table"),
    ],
)
def test_load_rejects_an_invalid_request(tmp_path: Path, document: Any, message: str):
    assert message in failure(tmp_path, document)


@pytest.mark.parametrize(
    "document, field",
    [
        ({"Function": {"info": info(), "colour": "blue"}}, "colour"),
        ({"Function": {"info": info(colour="blue")}}, "info.colour"),
        (function(input=[{"Table": input_table(colour="blue")}]), "input.0.colour"),
    ],
)
def test_load_rejects_undeclared_fields(tmp_path: Path, document: Any, field: str):
    assert field in failure(tmp_path, document)


def test_load_of_a_missing_file_carries_its_path(tmp_path: Path):
    missing = tmp_path / "nope.yaml"
    with pytest.raises(RequestFailure) as raised:
        load(missing)
    assert str(missing) in str(raised.value)
    assert raised.value.exit_code == RequestFailure.exit_code


def test_request_is_frozen():
    request = load(MINIMAL)
    with pytest.raises(ValidationError):
        request.info = None


def test_tables_built_from_models_are_kept():
    output = OutputTable.model_validate(output_table())
    request = Request(info=load(MINIMAL).info, output=(output,))
    assert request.output == (output,)


@pytest.mark.parametrize(
    "milliseconds, moment",
    [
        (0, datetime(1970, 1, 1, tzinfo=timezone.utc)),
        (1780000000100, datetime.fromtimestamp(1780000000.1, tz=timezone.utc)),
    ],
)
def test_triggered_on_datetime_is_utc(milliseconds: int, moment: datetime):
    triggered = Request.model_validate(
        {"info": info(triggered_on=milliseconds)},
    ).info.triggered_on_datetime
    assert triggered == moment
    assert triggered.tzinfo is timezone.utc


def test_streamer_request_loads_its_envelope(tmp_path: Path):
    streamer = {
        "project_id": "P-1",
        "project": "p",
        "collection_id": 19,
        "collection": "my_collection",
        "function_id": "F-1",
        "function": "stream_people",
        "function_version_id": "FV-1",
        "function_defined_on": 1780000000000,
        "function_bundle": {"uri": "file:///bundle.tar.zst", "env_prefix": "ROOT"},
        "function_stage": {"uri": "file:///tmp/stage", "env_prefix": "ROOT"},
        "worker_id": "W-1",
        "scheduled_on": 1780000000200,
    }
    path = written(tmp_path, {"Streamer": {"info": streamer}})
    assert StreamerRequest.load(path, RequestErrorCode.READ).info.worker_id == "W-1"


@pytest.mark.parametrize("envelope", ["Function", "Streamer"])
def test_any_request_reads_either_envelope(tmp_path: Path, envelope: str):
    path = written(tmp_path, {envelope: {"info": info(worker_id="W-1")}})
    request = AnyRequest.load(path, RequestErrorCode.READ)
    assert request.info.function_bundle.uri == "s3://bucket/bundle.tar.zst"


@pytest.mark.parametrize(
    "document",
    [
        {},
        {"Function": {"info": info()}, "Streamer": {"info": info()}},
    ],
)
def test_any_request_needs_exactly_one_envelope(tmp_path: Path, document: dict):
    with pytest.raises(RequestFailure, match="exactly one of Function or Streamer"):
        AnyRequest.load(written(tmp_path, document), RequestErrorCode.READ)


def test_a_location_without_prefix_names_no_mount():
    location = Location(uri="file:///data/bundle.tar.zst")

    declaration = location.mount_declaration()

    assert declaration == AnonymousMountDeclaration()


def test_a_location_with_prefix_names_its_mount():
    location = Location(uri="s3://bundles/one.tar.zst", env_prefix="BUNDLES")

    declaration = location.mount_declaration()

    assert declaration == EnvironmentMountDeclaration(id="BUNDLES")
