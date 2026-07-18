#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.dataquality._error import TableURIConfigurationError
from tabsdatak.dataquality._symbols import TableType
from tabsdatak.dataquality._tableinfo import TableInfo, TableKind
from tabsdatak.dataquality._tableuri import build_table_uri_object

pytestmark = pytest.mark.dq


def test_table_type_values_match_catalog_codes():
    assert TableType.Regular.value == "R"
    assert TableType.DQData.value == "DQD"
    assert TableType.DQSummary.value == "DQS"


def test_table_kind_carries_type_and_suffix():
    assert TableKind.DQ_ENRICH.type is TableType.DQData
    assert TableKind.DQ_ENRICH.suffix == "_dq_enrich"
    assert TableKind.DQ_SUMMARY.type is TableType.DQSummary
    assert TableKind.REGULAR.suffix is None


def test_table_info_validates_fields():
    info = TableInfo("src", "from_coll", TableKind.DQ_FILTER, "t", "coll", 0)
    assert info.table == "t"
    assert info.kind is TableKind.DQ_FILTER
    with pytest.raises(ValueError):
        TableInfo("src", "from_coll", TableKind.DQ_FILTER, "", "coll", 0)
    with pytest.raises(ValueError):
        TableInfo("src", "from_coll", TableKind.DQ_FILTER, "t", "coll", -1)


def test_build_table_uri_parses_collection_table_version():
    uri = build_table_uri_object("mycoll/mytable@HEAD")
    assert uri.collection == "mycoll"
    assert uri.table == "mytable"
    assert str(uri.version) == "HEAD"


def test_build_table_uri_bare_table():
    uri = build_table_uri_object("bare")
    assert uri.collection == ""
    assert uri.table == "bare"
    assert uri.version is None


def test_build_table_uri_rejects_malformed():
    with pytest.raises(TableURIConfigurationError):
        build_table_uri_object("a/b/c@x")
