from __future__ import annotations

from typing import get_args, get_origin

from tabsdatak.conn.common.types import DataType
from tabsdatak.conn.common.sql import types as _types


def test_column_name_spec_is_str():
    # PLACEHOLDER alias today; tighten when the real type lands.
    assert _types.ColumnNameSpec is str


def test_schema_override_spec_is_a_list_alias():
    assert get_origin(_types.SchemaOverrideSpec) is list


def test_schema_override_spec_inner_accepts_dict_or_none():
    # ``list[dict[ColumnNameSpec, DataType] | None]`` -- the inner
    # element type is ``dict[...] | None``.
    (inner,) = get_args(_types.SchemaOverrideSpec)
    arms = set(get_args(inner))
    assert type(None) in arms
    [dict_arm] = [a for a in arms if a is not type(None)]
    assert get_origin(dict_arm) is dict
    key_t, value_t = get_args(dict_arm)
    assert key_t is _types.ColumnNameSpec
    assert value_t is DataType


def test_schema_override_spec_is_assignable_at_runtime():
    # The alias resolves to a plain ``list``; runtime values can be
    # constructed directly (no validation wrapper today).
    value: _types.SchemaOverrideSpec = [
        {"a": DataType.INT, "b": DataType.STRING},
        None,
        {"c": DataType.DATETIME},
    ]
    assert len(value) == 3
    assert value[0]["a"] is DataType.INT
    assert value[1] is None
