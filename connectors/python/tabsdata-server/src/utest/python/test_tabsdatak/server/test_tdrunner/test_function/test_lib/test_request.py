#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from pathlib import Path

import pytest

from tabsdatak.server._tdrunner.function._lib.request import (
    InvalidRequestError,
    Location,
    Request,
    Table,
    load_request,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


RESOURCES = Path(__file__).parent / "resources"


def _write(tmp_path: Path, body: str, name: str = "request.yaml") -> Path:
    p = tmp_path / name
    p.write_text(body)
    return p


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
    assert req.output[0].location == Location(
        uri="s3://bucket/out/customers.parquet", env_prefix="AWS_"
    )
    assert req.system_input[0].name == "td-initial-values"
    assert req.system_output[0].location.uri == "s3://bucket/state/out.parquet"


def test_load_string_ids(tmp_path):
    yaml_text = """
info:
  collection: c
  collection_id: collection-uuid
  execution_id: exec-uuid
  transaction_id: tx-uuid
  function_run_id: run-uuid
  function_bundle:
    uri: file:///tmp/b.tgz
input: []
output: []
system_input: []
system_output: []
"""
    req = load_request(_write(tmp_path, yaml_text))
    assert req.info.collection_id == "collection-uuid"
    assert req.info.execution_id == "exec-uuid"
    assert req.info.transaction_id == "tx-uuid"
    assert req.info.function_run_id == "run-uuid"


def test_load_bundle_without_env_prefix(tmp_path):
    yaml_text = """
info:
  collection: c
  collection_id: 1
  execution_id: 2
  transaction_id: 3
  function_run_id: 4
  function_bundle:
    uri: file:///tmp/b.tgz
input: []
output: []
system_input: []
system_output: []
"""
    req = load_request(_write(tmp_path, yaml_text))
    assert req.info.function_bundle.env_prefix is None


def test_load_bundle_with_explicit_null_env_prefix(tmp_path):
    yaml_text = """
info:
  collection: c
  collection_id: 1
  execution_id: 2
  transaction_id: 3
  function_run_id: 4
  function_bundle:
    uri: file:///tmp/b.tgz
    env_prefix: null
input: []
output: []
system_input: []
system_output: []
"""
    req = load_request(_write(tmp_path, yaml_text))
    assert req.info.function_bundle.env_prefix is None


def test_load_table_without_location_key(tmp_path):
    yaml_text = """
info:
  collection: c
  collection_id: 1
  execution_id: 2
  transaction_id: 3
  function_run_id: 4
  function_bundle:
    uri: file:///b
output:
  - name: bare_table
input: []
system_input: []
system_output: []
"""
    req = load_request(_write(tmp_path, yaml_text))
    assert req.output == (Table(name="bare_table", location=None),)


def test_load_table_with_explicit_null_location(tmp_path):
    yaml_text = """
info:
  collection: c
  collection_id: 1
  execution_id: 2
  transaction_id: 3
  function_run_id: 4
  function_bundle:
    uri: file:///b
output:
  - name: bare_table
    location: null
input: []
system_input: []
system_output: []
"""
    req = load_request(_write(tmp_path, yaml_text))
    assert req.output[0].location is None


def test_load_omits_table_lists_default_to_empty(tmp_path):
    """If a top-level table list key is missing, treat as empty."""
    yaml_text = """
info:
  collection: c
  collection_id: 1
  execution_id: 2
  transaction_id: 3
  function_run_id: 4
  function_bundle:
    uri: file:///b
"""
    req = load_request(_write(tmp_path, yaml_text))
    assert req.input == ()
    assert req.output == ()
    assert req.system_input == ()
    assert req.system_output == ()


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
    p = _write(tmp_path, "input: []\n")
    with pytest.raises(InvalidRequestError, match="info"):
        load_request(p)


def test_load_info_not_a_mapping(tmp_path):
    p = _write(tmp_path, "info: not-a-dict\n")
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
    ],
)
def test_load_missing_required_info_field(tmp_path, missing_field):
    fields = {
        "collection": "c",
        "collection_id": 1,
        "execution_id": 2,
        "transaction_id": 3,
        "function_run_id": 4,
        "function_bundle": {"uri": "file:///b"},
    }
    fields.pop(missing_field)
    import yaml as pyyaml

    p = _write(tmp_path, pyyaml.safe_dump({"info": fields}))
    with pytest.raises(InvalidRequestError) as exc_info:
        load_request(p)
    assert missing_field in str(exc_info.value)


def test_load_collection_wrong_type(tmp_path):
    yaml_text = """
info:
  collection: 12345
  collection_id: 1
  execution_id: 2
  transaction_id: 3
  function_run_id: 4
  function_bundle:
    uri: file:///b
"""
    with pytest.raises(InvalidRequestError, match="info.collection"):
        load_request(_write(tmp_path, yaml_text))


def test_load_collection_id_wrong_type(tmp_path):
    yaml_text = """
info:
  collection: c
  collection_id: [1, 2, 3]
  execution_id: 2
  transaction_id: 3
  function_run_id: 4
  function_bundle:
    uri: file:///b
"""
    with pytest.raises(InvalidRequestError, match="info.collection_id"):
        load_request(_write(tmp_path, yaml_text))


def test_load_function_bundle_not_a_mapping(tmp_path):
    yaml_text = """
info:
  collection: c
  collection_id: 1
  execution_id: 2
  transaction_id: 3
  function_run_id: 4
  function_bundle: just-a-string
"""
    with pytest.raises(InvalidRequestError, match="function_bundle"):
        load_request(_write(tmp_path, yaml_text))


def test_load_location_missing_uri(tmp_path):
    yaml_text = """
info:
  collection: c
  collection_id: 1
  execution_id: 2
  transaction_id: 3
  function_run_id: 4
  function_bundle:
    env_prefix: AWS_
"""
    with pytest.raises(InvalidRequestError, match="uri"):
        load_request(_write(tmp_path, yaml_text))


def test_load_location_uri_wrong_type(tmp_path):
    yaml_text = """
info:
  collection: c
  collection_id: 1
  execution_id: 2
  transaction_id: 3
  function_run_id: 4
  function_bundle:
    uri: 42
"""
    with pytest.raises(InvalidRequestError, match="uri"):
        load_request(_write(tmp_path, yaml_text))


def test_load_location_env_prefix_wrong_type(tmp_path):
    yaml_text = """
info:
  collection: c
  collection_id: 1
  execution_id: 2
  transaction_id: 3
  function_run_id: 4
  function_bundle:
    uri: file:///b
    env_prefix: 42
"""
    with pytest.raises(InvalidRequestError, match="env_prefix"):
        load_request(_write(tmp_path, yaml_text))


# --- table-list errors ---


def test_load_table_list_not_a_list(tmp_path):
    yaml_text = """
info:
  collection: c
  collection_id: 1
  execution_id: 2
  transaction_id: 3
  function_run_id: 4
  function_bundle:
    uri: file:///b
output: not-a-list
"""
    with pytest.raises(InvalidRequestError, match="output"):
        load_request(_write(tmp_path, yaml_text))


def test_load_table_entry_not_a_mapping(tmp_path):
    yaml_text = """
info:
  collection: c
  collection_id: 1
  execution_id: 2
  transaction_id: 3
  function_run_id: 4
  function_bundle:
    uri: file:///b
output:
  - just-a-string
"""
    with pytest.raises(InvalidRequestError, match="output"):
        load_request(_write(tmp_path, yaml_text))


def test_load_table_missing_name(tmp_path):
    yaml_text = """
info:
  collection: c
  collection_id: 1
  execution_id: 2
  transaction_id: 3
  function_run_id: 4
  function_bundle:
    uri: file:///b
output:
  - location:
      uri: file:///t
"""
    with pytest.raises(InvalidRequestError, match="name"):
        load_request(_write(tmp_path, yaml_text))


def test_load_nested_table_location_uri_wrong_type(tmp_path):
    yaml_text = """
info:
  collection: c
  collection_id: 1
  execution_id: 2
  transaction_id: 3
  function_run_id: 4
  function_bundle:
    uri: file:///b
input:
  - name: a
    location:
      uri: 42
"""
    with pytest.raises(InvalidRequestError, match="uri"):
        load_request(_write(tmp_path, yaml_text))


# --- extra="forbid": unknown fields are rejected ---


def test_load_extra_field_at_top_level_is_forbidden(tmp_path):
    yaml_text = """
info:
  collection: c
  collection_id: 1
  execution_id: 2
  transaction_id: 3
  function_run_id: 4
  function_bundle:
    uri: file:///b
unknown_top_level: hi
"""
    with pytest.raises(InvalidRequestError, match="unknown_top_level"):
        load_request(_write(tmp_path, yaml_text))


def test_load_extra_field_in_info_is_forbidden(tmp_path):
    yaml_text = """
info:
  collection: c
  collection_id: 1
  execution_id: 2
  transaction_id: 3
  function_run_id: 4
  function_bundle:
    uri: file:///b
  surprise: 1
"""
    with pytest.raises(InvalidRequestError, match="surprise"):
        load_request(_write(tmp_path, yaml_text))


# --- frozen ---


def test_request_is_frozen():
    req = load_request(RESOURCES / "minimal_publisher_request.yaml")
    with pytest.raises(Exception):
        # frozen pydantic model: assignment raises ValidationError
        req.input = ()  # type: ignore[misc]


def test_location_is_frozen():
    loc = Location(uri="s3://x")
    with pytest.raises(Exception):
        loc.uri = "other"  # type: ignore[misc]


def test_table_is_frozen():
    t = Table(name="x")
    with pytest.raises(Exception):
        t.name = "other"  # type: ignore[misc]
