#
# Copyright 2026 Tabsdata Inc.
#

"""The column prefixes and names are declared as literals so their value is
readable at the point of definition. This module keeps those literals honest by
recomposing each one from the atomic parts it was built from.
"""

from __future__ import annotations

import pytest

from tabsdatak.tableframe._engine.constant.column import names, prefixes
from tabsdatak.tableframe._engine.constant.column.infixes import (
    TD_SYSTEM_COLUMN_INFIX,
)
from tabsdatak.tableframe._engine.constant.column.markers import (
    TD_VOLATILE_SYSTEM_COLUMN_MARKER,
)
from tabsdatak.tableframe._engine.constant.column.qualifier import (
    TD_SYSTEM_COLUMN_QUALIFIER,
)
from tabsdatak.tableframe._engine.constant.column.segments import (
    TD_FILE_ORIGIN_SYSTEM_COLUMN_SEGMENT,
    TD_OPTIONAL_SYSTEM_COLUMN_SEGMENT,
    TD_ORIGIN_SYSTEM_COLUMN_SEGMENT,
    TD_TIMESTAMP_OPTIONAL_SYSTEM_COLUMN_SEGMENT,
    TD_VERSION_SYSTEM_COLUMN_SEGMENT,
)
from tabsdatak.tableframe._engine.constant.column.sigils import (
    TD_CONNECTOR_SYSTEM_COLUMN_SIGIL,
    TD_STANDARD_SYSTEM_COLUMN_SIGIL,
)
from tabsdatak.tableframe._engine.constant.column.suffixes import (
    TD_EXECUTION_SYSTEM_COLUMN_SUFFIX,
    TD_FUNCTION_SYSTEM_COLUMN_SUFFIX,
    TD_IDENTIFIER_SYSTEM_COLUMN_SUFFIX,
    TD_INDEX_SYSTEM_COLUMN_SUFFIX,
    TD_ITEM_SYSTEM_COLUMN_SUFFIX,
    TD_MAXIMUM_SYSTEM_COLUMN_SUFFIX,
    TD_MINIMUM_SYSTEM_COLUMN_SUFFIX,
    TD_OFFSET_SYSTEM_COLUMN_SUFFIX,
    TD_PROVENANCE_SYSTEM_COLUMN_SUFFIX,
    TD_ROW_INDEX_SYSTEM_COLUMN_SUFFIX,
    TD_SERVER_SYSTEM_COLUMN_SUFFIX,
    TD_TIMESTAMP_SYSTEM_COLUMN_SUFFIX,
    TD_TIMESTAMP_VALUE_SYSTEM_COLUMN_SUFFIX,
    TD_TRANSACTION_SYSTEM_COLUMN_SUFFIX,
    TD_USER_SYSTEM_COLUMN_SUFFIX,
    TD_VERSION_VALUE_SYSTEM_COLUMN_SUFFIX,
)

pytestmark = pytest.mark.tableframe

_STANDARD_PREFIX = (
    f"{TD_STANDARD_SYSTEM_COLUMN_SIGIL}"
    f"{TD_SYSTEM_COLUMN_INFIX}"
    f"{TD_SYSTEM_COLUMN_QUALIFIER}"
)
_CONNECTOR_PREFIX = (
    f"{TD_CONNECTOR_SYSTEM_COLUMN_SIGIL}"
    f"{TD_SYSTEM_COLUMN_INFIX}"
    f"{TD_SYSTEM_COLUMN_QUALIFIER}"
)
_VERSION_PREFIX = (
    f"{_STANDARD_PREFIX}{TD_VERSION_SYSTEM_COLUMN_SEGMENT}{TD_SYSTEM_COLUMN_QUALIFIER}"
)
_OPTIONAL_PREFIX = (
    f"{_STANDARD_PREFIX}{TD_OPTIONAL_SYSTEM_COLUMN_SEGMENT}{TD_SYSTEM_COLUMN_QUALIFIER}"
)
_VOLATILE_PREFIX = f"{_STANDARD_PREFIX}{TD_VOLATILE_SYSTEM_COLUMN_MARKER}"
_ORIGIN_PREFIX = (
    f"{_CONNECTOR_PREFIX}{TD_ORIGIN_SYSTEM_COLUMN_SEGMENT}{TD_SYSTEM_COLUMN_QUALIFIER}"
)
_OPTIONAL_TIMESTAMP_PREFIX = (
    f"{_OPTIONAL_PREFIX}"
    f"{TD_TIMESTAMP_OPTIONAL_SYSTEM_COLUMN_SEGMENT}"
    f"{TD_SYSTEM_COLUMN_QUALIFIER}"
)
_FILE_ORIGIN_PREFIX = (
    f"{_ORIGIN_PREFIX}"
    f"{TD_FILE_ORIGIN_SYSTEM_COLUMN_SEGMENT}"
    f"{TD_SYSTEM_COLUMN_QUALIFIER}"
)

PREFIXES = [
    ("TD_SYSTEM_COLUMN_PREFIX", _STANDARD_PREFIX),
    ("TD_CONNECTOR_SYSTEM_COLUMN_PREFIX", _CONNECTOR_PREFIX),
    ("TD_VERSION_SYSTEM_COLUMN_PREFIX", _VERSION_PREFIX),
    ("TD_OPTIONAL_SYSTEM_COLUMN_PREFIX", _OPTIONAL_PREFIX),
    ("TD_VOLATILE_SYSTEM_COLUMN_PREFIX", _VOLATILE_PREFIX),
    ("TD_ORIGIN_SYSTEM_COLUMN_PREFIX", _ORIGIN_PREFIX),
    ("TD_OPTIONAL_TIMESTAMP_SYSTEM_COLUMN_PREFIX", _OPTIONAL_TIMESTAMP_PREFIX),
    ("TD_FILE_ORIGIN_SYSTEM_COLUMN_PREFIX", _FILE_ORIGIN_PREFIX),
]

NAMES = [
    (
        "TD_IDENTIFIER_SYSTEM_COLUMN_NAME",
        f"{_STANDARD_PREFIX}{TD_IDENTIFIER_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "TD_OFFSET_SYSTEM_COLUMN_NAME",
        f"{_STANDARD_PREFIX}{TD_OFFSET_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "TD_ROW_INDEX_SYSTEM_COLUMN_NAME",
        f"{_STANDARD_PREFIX}{TD_ROW_INDEX_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "TD_PROVENANCE_SYSTEM_COLUMN_NAME",
        f"{_STANDARD_PREFIX}{TD_PROVENANCE_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "TD_EXECUTION_SYSTEM_COLUMN_NAME",
        f"{_VERSION_PREFIX}{TD_EXECUTION_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "TD_TRANSACTION_SYSTEM_COLUMN_NAME",
        f"{_VERSION_PREFIX}{TD_TRANSACTION_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "TD_VERSION_SYSTEM_COLUMN_NAME",
        f"{_VERSION_PREFIX}{TD_VERSION_VALUE_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "TD_TIMESTAMP_SYSTEM_COLUMN_NAME",
        f"{_VERSION_PREFIX}{TD_TIMESTAMP_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "TD_SERVER_TIMESTAMP_SYSTEM_COLUMN_NAME",
        f"{_OPTIONAL_TIMESTAMP_PREFIX}{TD_SERVER_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "TD_USER_TIMESTAMP_SYSTEM_COLUMN_NAME",
        f"{_OPTIONAL_TIMESTAMP_PREFIX}{TD_USER_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "TD_VALUE_TIMESTAMP_SYSTEM_COLUMN_NAME",
        f"{_OPTIONAL_TIMESTAMP_PREFIX}{TD_TIMESTAMP_VALUE_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "TD_FUNCTION_TIMESTAMP_SYSTEM_COLUMN_NAME",
        f"{_OPTIONAL_TIMESTAMP_PREFIX}{TD_FUNCTION_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "TD_INDEX_SYSTEM_COLUMN_NAME",
        f"{_VOLATILE_PREFIX}{TD_INDEX_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "TD_ITEM_SYSTEM_COLUMN_NAME",
        f"{_VOLATILE_PREFIX}{TD_ITEM_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "TD_MINIMUM_SYSTEM_COLUMN_NAME",
        f"{_VOLATILE_PREFIX}{TD_MINIMUM_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "TD_MAXIMUM_SYSTEM_COLUMN_NAME",
        f"{_VOLATILE_PREFIX}{TD_MAXIMUM_SYSTEM_COLUMN_SUFFIX}",
    ),
]


@pytest.mark.parametrize("constant,composed", PREFIXES, ids=[p[0] for p in PREFIXES])
def test_prefix_matches_its_composition(constant, composed):
    assert getattr(prefixes, constant) == composed


@pytest.mark.parametrize("constant,composed", NAMES, ids=[n[0] for n in NAMES])
def test_name_matches_its_composition(constant, composed):
    assert getattr(names, constant) == composed


def test_every_declared_prefix_is_covered():
    declared = {
        name
        for name in vars(prefixes)
        if name.endswith("_PREFIX") and name != "TD_TEMP_COLUMN_PREFIX"
    }
    assert declared == {constant for constant, _ in PREFIXES}


def test_every_declared_name_is_covered():
    declared = {name for name in vars(names) if name.endswith("_NAME")}
    assert declared == {constant for constant, _ in NAMES}


def test_namespaced_prefixes_are_the_version_and_optional_prefixes():
    assert prefixes.TD_NAMESPACED_SYSTEM_COLUMN_PREFIXES == (
        _VERSION_PREFIX,
        _OPTIONAL_PREFIX,
    )


def test_namespaced_virtual_prefixes_are_the_version_prefix():
    assert prefixes.TD_NAMESPACED_VIRTUAL_SYSTEM_COLUMN_PREFIXES == (_VERSION_PREFIX,)
