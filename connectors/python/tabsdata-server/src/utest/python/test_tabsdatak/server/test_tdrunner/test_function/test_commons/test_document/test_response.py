#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from tabsdatak.common._configuration.document.error import DocumentException
from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._commons.document.request import Request
from tabsdatak.server._tdrunner.function._commons.document.response import (
    Response,
    TableEntry,
    WrittenNoData,
    WrittenTable,
)
from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputEntry,
    OutputManifest,
    TableStats,
)
from tabsdatak.server._tdrunner.function._lib.errors import CommandRuntimeException

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

INFO = TableStats(column_count=3, row_count=10, schema_hash="sha256:abc", data_bytes=64)
MINIMAL = Path(__file__).parent / "resources" / "minimal_publisher_request.yaml"


class MismatchFailure(CommandRuntimeException):
    exit_code = 30


class ResponseErrorCode(ErrorCode):
    MISMATCH = ErrorDef(MismatchFailure, "Output {table} is missing from the manifest")
    READ = ErrorDef(MismatchFailure, "Cannot read the request {path}: {detail}")


def written(tmp_path: Path, document: Any) -> Path:
    path = tmp_path / "response.yaml"
    path.write_text(document if isinstance(document, str) else yaml.safe_dump(document))
    return path


@pytest.mark.parametrize(
    "response",
    [
        Response(output=()),
        Response(output=(TableEntry(table="customers", info=INFO),)),
        Response(output=(TableEntry(table="orders"),)),
        Response(
            output=(
                TableEntry(table="customers", info=INFO),
                TableEntry(table="orders"),
            ),
        ),
    ],
)
def test_response_round_trips(tmp_path: Path, response: Response):
    path = tmp_path / "nested" / "response.yaml"
    response.write(path)
    assert Response.load(path) == response


def test_write_produces_plain_yaml_without_absent_fields(tmp_path: Path):
    path = tmp_path / "response.yaml"
    Response(output=(TableEntry(table="orders"),)).write(path)
    assert "!!" not in path.read_text()
    assert yaml.safe_load(path.read_text()) == {"output": [{"table": "orders"}]}


def test_write_to_a_folder_fails(tmp_path: Path):
    with pytest.raises(OSError):
        Response(output=()).write(tmp_path)


def test_function_output_tags_each_written_table():
    response = Response(
        output=(TableEntry(table="customers", info=INFO), TableEntry(table="orders")),
    )
    assert response.function_output().document() == {
        "output": [
            {
                "Data": {
                    "table": "customers",
                    "info": {
                        "column_count": 3,
                        "row_count": 10,
                        "schema_hash": "sha256:abc",
                        "data_bytes": 64,
                    },
                },
            },
            {"NoData": {"table": "orders"}},
        ],
    }


def test_build_takes_the_stats_of_outputs_and_state():
    request = Request.load(MINIMAL, ResponseErrorCode.READ)
    manifest = OutputManifest(
        outputs=(OutputEntry(name="customers", uri="s3://b/c.t", stats=INFO),)
    )
    response = Response.build(
        request, manifest, {"td-initial-values": INFO}, ResponseErrorCode.MISMATCH
    )
    assert response.output == (
        TableEntry(table="customers", info=INFO),
        TableEntry(table="td-initial-values", info=INFO),
    )


def test_build_reports_no_data_without_stats():
    request = Request.load(MINIMAL, ResponseErrorCode.READ)
    manifest = OutputManifest(outputs=(OutputEntry(name="customers"),))
    response = Response.build(request, manifest, {}, ResponseErrorCode.MISMATCH)
    assert response.output == (
        TableEntry(table="customers"),
        TableEntry(table="td-initial-values"),
    )


def test_build_rejects_an_output_missing_from_the_manifest(tmp_path: Path):
    request = Request.load(MINIMAL, ResponseErrorCode.READ)
    with pytest.raises(MismatchFailure, match="customers"):
        Response.build(
            request,
            OutputManifest(outputs=()),
            {},
            ResponseErrorCode.MISMATCH,
        )


@pytest.mark.parametrize(
    "document, message",
    [
        ("output: { unclosed", "Cannot read the document"),
        ([1], "does not match its model"),
        ({}, "output"),
        ({"output": {"table": "t"}}, "output"),
        ({"output": [{"info": None}]}, "table"),
        ({"output": [{"table": "t", "info": {"column_count": "x"}}]}, "column_count"),
        ({"output": [], "colour": "blue"}, "colour"),
        ({"output": [{"table": "t", "colour": "blue"}]}, "output.0.colour"),
    ],
)
def test_load_rejects_an_invalid_response(tmp_path: Path, document: Any, message: str):
    with pytest.raises(DocumentException) as raised:
        Response.load(written(tmp_path, document))
    assert message in str(raised.value)


def test_response_is_frozen():
    with pytest.raises(ValidationError):
        Response(output=()).output = ()


@pytest.mark.parametrize(
    "variants",
    [{}, {"NoData": WrittenNoData(table="t"), "Data": {"table": "t", "info": INFO}}],
)
def test_written_table_is_exactly_one_variant(variants: dict):
    with pytest.raises(ValidationError, match="exactly one of NoData or Data"):
        WrittenTable(**variants)
