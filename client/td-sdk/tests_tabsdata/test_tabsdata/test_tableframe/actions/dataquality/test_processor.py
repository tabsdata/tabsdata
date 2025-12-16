#
# Copyright 2025 Tabs Data Inc.
#

import pytest

from tabsdata._tableinfo import TableKind
from tabsdata._tableuri import ExtendedTableURI, TableURI
from tabsdata.dataquality import DataQuality, Filter, Select, Summary
from tabsdata.exceptions import (
    DataQualityConfigurationError,
    DataQualityProcessingError,
)
from tabsdata.tableframe._actions.dataquality.processor import DataQualityProcessor


class TestDataQualityProcessorTableNames:
    def test_table_names_empty_actions(self):
        processor = DataQualityProcessor(
            collection="collection",
            input_tables={},
            output_tables=[],
            input_actions=[],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert input_tables_names == []
        assert output_tables_names == []

    def test_table_names_single_input_action_with_select(self):
        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "input_table1",
                    None,
                ),
                0,
            )
        }

        dq_action = DataQuality(
            "input1",
            classifiers=[],
            operations=[Select(to_table="input1_select")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 1
        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "input1_select"
        assert input_tables_names[0].kind == TableKind.DQ_SELECT
        assert input_tables_names[0].from_table == "input_table1"
        assert input_tables_names[0].from_collection == "collection"
        assert output_tables_names == []

    def test_table_names_single_input_action_with_filter(self):
        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "input_table1",
                    None,
                ),
                0,
            )
        }

        dq_action = DataQuality(
            "input1",
            classifiers=[],
            operations=[Filter(to_table="input1_filter")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 1
        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "input1_filter"
        assert input_tables_names[0].kind == TableKind.DQ_FILTER
        assert input_tables_names[0].from_table == "input_table1"
        assert input_tables_names[0].from_collection == "collection"
        assert output_tables_names == []

    def test_table_names_single_input_action_with_summary(self):
        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "input_table1",
                    None,
                ),
                0,
            )
        }

        dq_action = DataQuality(
            "input1",
            classifiers=[],
            operations=[Summary(table="input1_summary")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 1
        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "input1_summary"
        assert input_tables_names[0].kind == TableKind.DQ_SUMMARY
        assert input_tables_names[0].from_table == "input_table1"
        assert input_tables_names[0].from_collection == "collection"
        assert output_tables_names == []

    def test_table_names_multiple_operations_in_single_action(self):
        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "input_table1",
                    None,
                ),
                0,
            )
        }

        dq_action = DataQuality(
            "input1",
            classifiers=[],
            operations=[
                Select(to_table="input1_select"),
                Filter(to_table="input1_filter"),
                Summary(table="input1_summary"),
            ],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 3
        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "input1_select"
        assert input_tables_names[0].kind == TableKind.DQ_SELECT
        assert input_tables_names[1].collection == "collection"
        assert input_tables_names[1].table == "input1_filter"
        assert input_tables_names[1].kind == TableKind.DQ_FILTER
        assert input_tables_names[2].collection == "collection"
        assert input_tables_names[2].table == "input1_summary"
        assert input_tables_names[2].kind == TableKind.DQ_SUMMARY
        assert output_tables_names == []

    def test_table_names_multiple_input_actions(self):
        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "input_table1",
                    None,
                ),
                0,
            ),
            "input2": ExtendedTableURI(
                TableURI(
                    None,
                    "input_table2",
                    None,
                ),
                1,
            ),
        }

        dq_action1 = DataQuality(
            "input1",
            classifiers=[],
            operations=[Select(to_table="input1_select")],
        )

        dq_action2 = DataQuality(
            "input2",
            classifiers=[],
            operations=[Filter(to_table="input2_filter")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action1, dq_action2],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 2
        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "input1_select"
        assert input_tables_names[0].kind == TableKind.DQ_SELECT
        assert input_tables_names[1].collection == "collection"
        assert input_tables_names[1].table == "input2_filter"
        assert input_tables_names[1].kind == TableKind.DQ_FILTER
        assert output_tables_names == []

    def test_table_names_simple_table_name_format(self):
        input_tables = {
            "table": ExtendedTableURI(
                TableURI(
                    None,
                    "table",
                    None,
                ),
                0,
            )
        }

        dq_action = DataQuality(
            "table",
            classifiers=[],
            operations=[Select(to_table="table_select")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 1
        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "table_select"
        assert input_tables_names[0].kind == TableKind.DQ_SELECT
        assert output_tables_names == []

    def test_table_names_collection_slash_table_format(self):
        input_tables = {
            "alias": ExtendedTableURI(
                TableURI(
                    "collection",
                    "table",
                    None,
                ),
                0,
            ),
        }

        dq_action = DataQuality(
            "collection/table",
            classifiers=[],
            operations=[Select(to_table="qualified_select")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 1
        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "qualified_select"
        assert input_tables_names[0].kind == TableKind.DQ_SELECT
        assert output_tables_names == []

    def test_table_names_table_at_version_format(self):
        input_tables = {
            "alias": ExtendedTableURI(
                TableURI(
                    None,
                    "table",
                    "HEAD",
                ),
                0,
            )
        }

        dq_action = DataQuality(
            "table@HEAD",
            classifiers=[],
            operations=[Select(to_table="versioned_select")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 1
        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "versioned_select"
        assert input_tables_names[0].kind == TableKind.DQ_SELECT
        assert output_tables_names == []

    def test_table_names_full_uri_format(self):
        input_tables = {
            "alias": ExtendedTableURI(
                TableURI(
                    "collection",
                    "table",
                    "HEAD~1",
                ),
                0,
            ),
        }

        dq_action = DataQuality(
            "collection/table@HEAD~1",
            classifiers=[],
            operations=[Select(to_table="full_uri_select")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 1
        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "full_uri_select"
        assert input_tables_names[0].kind == TableKind.DQ_SELECT
        assert output_tables_names == []

    def test_table_names_version_with_carets_format(self):
        input_tables = {
            "alias": ExtendedTableURI(
                TableURI(
                    None,
                    "table",
                    "HEAD^^",
                ),
                0,
            )
        }

        dq_action = DataQuality(
            "table@HEAD^^",
            classifiers=[],
            operations=[Select(to_table="caret_version_select")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 1
        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "caret_version_select"
        assert input_tables_names[0].kind == TableKind.DQ_SELECT
        assert output_tables_names == []

    def test_table_names_version_with_hash_format(self):
        input_tables = {
            "alias1": ExtendedTableURI(
                TableURI(
                    None,
                    "table",
                    "ABCDEFGHIJKLMNOPQRSTUVWXY0",
                ),
                0,
            )
        }

        dq_action = DataQuality(
            "table@ABCDEFGHIJKLMNOPQRSTUVWXY0",
            classifiers=[],
            operations=[Select(to_table="hash_version_select")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 1
        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "hash_version_select"
        assert input_tables_names[0].kind == TableKind.DQ_SELECT
        assert output_tables_names == []

    def test_name_collision_resolved_via_alias(self):
        input_tables = {
            "table_a": ExtendedTableURI(
                TableURI(
                    None,
                    "customers",
                    "HEAD",
                ),
                0,
            ),
            "table_b": ExtendedTableURI(
                TableURI(
                    None,
                    "customers",
                    "HEAD~1",
                ),
                1,
            ),
        }

        dq_action = DataQuality(
            "table_a",
            classifiers=[],
            operations=[Select(to_table="alias_resolved_select")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 1
        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "alias_resolved_select"
        assert input_tables_names[0].kind == TableKind.DQ_SELECT
        assert output_tables_names == []

    def test_name_collision_resolved_via_qualified_name_single_match(self):
        input_tables = {
            "alias": ExtendedTableURI(
                TableURI(
                    "collection_a",
                    "customers",
                    None,
                ),
                0,
            ),
        }

        dq_action = DataQuality(
            "collection_a/customers",
            classifiers=[],
            operations=[Select(to_table="qualified_resolved_select")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 1
        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "qualified_resolved_select"
        assert input_tables_names[0].kind == TableKind.DQ_SELECT
        assert output_tables_names == []

    def test_name_collision_resolved_different_collections(self):
        input_tables = {
            "table_a": ExtendedTableURI(
                TableURI(
                    "collection_a",
                    "customers",
                    None,
                ),
                0,
            ),
            "table_b": ExtendedTableURI(
                TableURI(
                    "collection_b",
                    "customers",
                    None,
                ),
                1,
            ),
        }

        dq_action = DataQuality(
            "collection_a/customers",
            classifiers=[],
            operations=[Select(to_table="different_collection_select")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 1
        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "different_collection_select"
        assert input_tables_names[0].kind == TableKind.DQ_SELECT
        assert output_tables_names == []

    def test_name_collision_resolved_using_default_collection(self):
        input_tables = {
            "alias": ExtendedTableURI(
                TableURI(
                    None,
                    "customers",
                    None,
                ),
                0,
            ),
        }

        dq_action = DataQuality(
            "customers",
            classifiers=[],
            operations=[Select(to_table="default_collection_select")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 1
        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "default_collection_select"
        assert input_tables_names[0].kind == TableKind.DQ_SELECT
        assert output_tables_names == []

    def test_name_collision_resolved_with_version_via_alias(self):
        input_tables = {
            "latest": ExtendedTableURI(
                TableURI(
                    None,
                    "customers",
                    "HEAD",
                ),
                0,
            ),
            "previous": ExtendedTableURI(
                TableURI(
                    None,
                    "customers",
                    "HEAD~1",
                ),
                1,
            ),
            "older": ExtendedTableURI(
                TableURI(
                    None,
                    "customers",
                    "HEAD~2",
                ),
                2,
            ),
        }

        dq_action = DataQuality(
            "latest",
            classifiers=[],
            operations=[Select(to_table="version_alias_select")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 1
        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "version_alias_select"
        assert input_tables_names[0].kind == TableKind.DQ_SELECT
        assert output_tables_names == []

    def test_name_collision_unresolved_multiple_versions_same_collection(self):
        input_tables = {
            "alias1": ExtendedTableURI(
                TableURI(
                    None,
                    "customers",
                    "HEAD",
                ),
                0,
            ),
            "alias2": ExtendedTableURI(
                TableURI(
                    None,
                    "customers",
                    "HEAD~1",
                ),
                1,
            ),
        }

        dq_action = DataQuality(
            "customers",
            classifiers=[],
            operations=[Select(to_table="unresolved_select")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        with pytest.raises(DataQualityProcessingError) as exception:
            processor.table_names()

        assert exception.value.code == "DQPE-001"
        assert "customers" in str(exception.value)

    def test_name_collision_unresolved_table_not_found(self):
        input_tables = {
            "alias": ExtendedTableURI(
                TableURI(
                    None,
                    "customers",
                    None,
                ),
                0,
            ),
        }

        dq_action = DataQuality(
            "products",
            classifiers=[],
            operations=[Select(to_table="not_found_select")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        with pytest.raises(DataQualityProcessingError) as exception:
            processor.table_names()

        assert exception.value.code == "DQPE-001"
        assert "products" in str(exception.value)

    def test_name_collision_unresolved_wrong_collection(self):
        input_tables = {
            "alias": ExtendedTableURI(
                TableURI(
                    "collection_a",
                    "customers",
                    None,
                ),
                0,
            ),
        }

        dq_action = DataQuality(
            "collection_b/customers",
            classifiers=[],
            operations=[Select(to_table="wrong_collection_select")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        with pytest.raises(DataQualityProcessingError) as exception:
            processor.table_names()

        assert exception.value.code == "DQPE-001"
        assert "collection_b/customers" in str(exception.value)

    def test_name_collision_unresolved_ambiguous_qualified_name(self):
        input_tables = {
            "alias1": ExtendedTableURI(
                TableURI(
                    "collection",
                    "customers",
                    "HEAD",
                ),
                0,
            ),
            "alias2": ExtendedTableURI(
                TableURI(
                    "collection",
                    "customers",
                    "HEAD~1",
                ),
                1,
            ),
        }

        dq_action = DataQuality(
            "collection/customers",
            classifiers=[],
            operations=[Select(to_table="ambiguous_select")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        with pytest.raises(DataQualityProcessingError) as exception:
            processor.table_names()

        assert exception.value.code == "DQPE-001"

    def test_name_collision_unresolved_empty_input_tables(self):
        input_tables = {}

        dq_action = DataQuality(
            "customers",
            classifiers=[],
            operations=[Select(to_table="empty_tables_select")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        with pytest.raises(DataQualityProcessingError) as exception:
            processor.table_names()

        assert exception.value.code == "DQPE-001"
        assert "customers" in str(exception.value)

    def test_output_table_resolved_successfully(self):
        output_tables = ["output1", "output2"]

        dq_action = DataQuality(
            "output1",
            classifiers=[],
            operations=[Select(to_table="output_select")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables={},
            output_tables=output_tables,
            input_actions=[],
            output_actions=[dq_action],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(output_tables_names) == 1
        assert output_tables_names[0].collection == "collection"
        assert output_tables_names[0].table == "output_select"
        assert output_tables_names[0].kind == TableKind.DQ_SELECT
        assert input_tables_names == []

    def test_output_table_unresolved_not_in_output_tables(self):
        output_tables = ["output1", "output2"]

        dq_action = DataQuality(
            "output3",
            classifiers=[],
            operations=[Select(to_table="unresolved_output_select")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables={},
            output_tables=output_tables,
            input_actions=[],
            output_actions=[dq_action],
        )

        with pytest.raises(DataQualityProcessingError) as exception:
            processor.table_names()

        assert "output3" in str(exception.value)

    def test_filter_with_none_to_table_does_not_produce_table(self):

        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "customers",
                    None,
                ),
                0,
            )
        }

        dq_action = DataQuality(
            "input1",
            classifiers=[],
            operations=[Filter()],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 0
        assert output_tables_names == []

    def test_table_names_with_none_table_in_summary(self):
        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "customers",
                    None,
                ),
                0,
            )
        }

        dq_action = DataQuality(
            "input1",
            classifiers=[],
            operations=[Summary(table=None)],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 1
        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "customers_dq_summary"
        assert input_tables_names[0].kind == TableKind.DQ_SUMMARY
        assert output_tables_names == []

    def test_table_names_complex_scenario_mixed_operations(self):
        input_tables = {
            "customers_latest": ExtendedTableURI(
                TableURI(
                    "sales",
                    "customers",
                    "HEAD",
                ),
                0,
            ),
            "orders_latest": ExtendedTableURI(
                TableURI(
                    "sales",
                    "orders",
                    "HEAD",
                ),
                1,
            ),
            "products": ExtendedTableURI(
                TableURI(
                    None,
                    "products",
                    None,
                ),
                2,
            ),
        }

        output_tables = ["cleaned_customers", "cleaned_orders"]

        input_action1 = DataQuality(
            "customers_latest",
            classifiers=[],
            operations=[
                Select(to_table="customers_select"),
                Filter(to_table="customers_filter"),
                Summary(table="customers_summary"),
            ],
        )

        input_action2 = DataQuality(
            "orders_latest",
            classifiers=[],
            operations=[
                Filter(to_table="orders_filter"),
            ],
        )

        output_action1 = DataQuality(
            "cleaned_customers",
            classifiers=[],
            operations=[
                Summary(table="output_summary"),
            ],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=output_tables,
            input_actions=[input_action1, input_action2],
            output_actions=[output_action1],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 4
        assert len(output_tables_names) == 1

        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "customers_select"
        assert input_tables_names[0].kind == TableKind.DQ_SELECT

        assert input_tables_names[1].collection == "collection"
        assert input_tables_names[1].table == "customers_filter"
        assert input_tables_names[1].kind == TableKind.DQ_FILTER

        assert input_tables_names[2].collection == "collection"
        assert input_tables_names[2].table == "customers_summary"
        assert input_tables_names[2].kind == TableKind.DQ_SUMMARY

        assert input_tables_names[3].collection == "collection"
        assert input_tables_names[3].table == "orders_filter"
        assert input_tables_names[3].kind == TableKind.DQ_FILTER

        assert output_tables_names[0].collection == "collection"
        assert output_tables_names[0].table == "output_summary"
        assert output_tables_names[0].kind == TableKind.DQ_SUMMARY

    def test_table_names_preserves_collection_in_result(self):
        input_tables = {
            "table1": ExtendedTableURI(
                TableURI(
                    "custom_collection",
                    "data",
                    None,
                ),
                0,
            ),
        }

        dq_action = DataQuality(
            "table1",
            classifiers=[],
            operations=[Select(to_table="collection_test_select")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 1
        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "collection_test_select"
        assert input_tables_names[0].kind == TableKind.DQ_SELECT
        assert output_tables_names == []

    def test_table_names_with_special_characters_in_names(self):
        input_tables = {
            "alias": ExtendedTableURI(
                TableURI(
                    None,
                    "table_123",
                    None,
                ),
                0,
            ),
        }

        dq_action = DataQuality(
            "table_123",
            classifiers=[],
            operations=[Select(to_table="special_chars_456")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 1
        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "special_chars_456"
        assert input_tables_names[0].kind == TableKind.DQ_SELECT
        assert output_tables_names == []

    def test_table_names_all_three_operation_types_together(self):
        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "source_table",
                    None,
                ),
                0,
            )
        }

        dq_action = DataQuality(
            "input1",
            classifiers=[],
            operations=[
                Select(to_table="mixed_select"),
                Filter(to_table="mixed_filter"),
                Summary(table="mixed_summary"),
            ],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 3

        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "mixed_select"
        assert input_tables_names[0].kind == TableKind.DQ_SELECT

        assert input_tables_names[1].collection == "collection"
        assert input_tables_names[1].table == "mixed_filter"
        assert input_tables_names[1].kind == TableKind.DQ_FILTER

        assert input_tables_names[2].collection == "collection"
        assert input_tables_names[2].table == "mixed_summary"
        assert input_tables_names[2].kind == TableKind.DQ_SUMMARY

        assert output_tables_names == []

    def test_select_requires_to_table_parameter(self):

        with pytest.raises(TypeError) as exc_info:
            # noinspection PyArgumentList
            Select()

        assert "missing 1 required positional argument: 'to_table'" in str(
            exc_info.value
        )

    def test_filter_with_none_to_table_produces_no_table(self):

        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "source_table",
                    None,
                ),
                0,
            )
        }

        dq_action = DataQuality(
            "input1",
            classifiers=[],
            operations=[Filter()],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 0
        assert output_tables_names == []

    def test_table_names_summary_with_default_name(self):
        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "source_table",
                    None,
                ),
                0,
            )
        }

        dq_action = DataQuality(
            "input1",
            classifiers=[],
            operations=[Summary(table=None)],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 1
        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "source_table_dq_summary"
        assert input_tables_names[0].kind == TableKind.DQ_SUMMARY
        assert output_tables_names == []

    def test_table_names_mixed_custom_and_default_names(self):

        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "source_data",
                    None,
                ),
                0,
            )
        }

        dq_action = DataQuality(
            "input1",
            classifiers=[],
            operations=[
                Select(to_table="custom_select"),
                Filter(),
                Summary(table="custom_summary"),
            ],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 2

        assert input_tables_names[0].collection == "collection"
        assert input_tables_names[0].table == "custom_select"
        assert input_tables_names[0].kind == TableKind.DQ_SELECT

        assert input_tables_names[1].collection == "collection"
        assert input_tables_names[1].table == "custom_summary"
        assert input_tables_names[1].kind == TableKind.DQ_SUMMARY

        assert output_tables_names == []

    def test_table_names_many_input_actions_many_operations(self):

        input_tables = {
            "table1": ExtendedTableURI(
                TableURI(
                    None,
                    "sales_data",
                    None,
                ),
                0,
            ),
            "table2": ExtendedTableURI(
                TableURI(
                    None,
                    "inventory_data",
                    None,
                ),
                1,
            ),
            "table3": ExtendedTableURI(
                TableURI(
                    None,
                    "customer_data",
                    None,
                ),
                2,
            ),
        }

        dq_action1 = DataQuality(
            "table1",
            classifiers=[],
            operations=[
                Select(to_table="sales_valid"),
                Filter(to_table="sales_invalid"),
                Summary(table="sales_report"),
            ],
        )

        dq_action2 = DataQuality(
            "table2",
            classifiers=[],
            operations=[
                Select(to_table="inventory_valid"),
                Filter(),
                Summary(table="inventory_report"),
            ],
        )

        dq_action3 = DataQuality(
            "table3",
            classifiers=[],
            operations=[
                Select(to_table="customer_valid"),
                Filter(to_table="customer_invalid"),
                Summary(table=None),
            ],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action1, dq_action2, dq_action3],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 8

        assert input_tables_names[0].table == "sales_valid"
        assert input_tables_names[0].kind == TableKind.DQ_SELECT
        assert input_tables_names[1].table == "sales_invalid"
        assert input_tables_names[1].kind == TableKind.DQ_FILTER
        assert input_tables_names[2].table == "sales_report"
        assert input_tables_names[2].kind == TableKind.DQ_SUMMARY

        assert input_tables_names[3].table == "inventory_valid"
        assert input_tables_names[3].kind == TableKind.DQ_SELECT
        assert input_tables_names[4].table == "inventory_report"
        assert input_tables_names[4].kind == TableKind.DQ_SUMMARY

        assert input_tables_names[5].table == "customer_valid"
        assert input_tables_names[5].kind == TableKind.DQ_SELECT
        assert input_tables_names[6].table == "customer_invalid"
        assert input_tables_names[6].kind == TableKind.DQ_FILTER
        assert input_tables_names[7].table == "customer_data_dq_summary"
        assert input_tables_names[7].kind == TableKind.DQ_SUMMARY

        assert output_tables_names == []

    def test_table_names_many_output_actions_many_operations(self):

        output_tables = ["output1", "output2", "output3"]

        dq_action1 = DataQuality(
            "output1",
            classifiers=[],
            operations=[
                Select(to_table="output1_select"),
                Filter(to_table="output1_filter"),
                Summary(table="output1_summary"),
            ],
        )

        dq_action2 = DataQuality(
            "output2",
            classifiers=[],
            operations=[
                Select(to_table="output2_select"),
                Filter(to_table="output2_filter"),
                Summary(table=None),
            ],
        )

        dq_action3 = DataQuality(
            "output3",
            classifiers=[],
            operations=[
                Select(to_table="output3_select"),
                Filter(),
                Summary(table="output3_summary"),
            ],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables={},
            output_tables=output_tables,
            input_actions=[],
            output_actions=[dq_action1, dq_action2, dq_action3],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(output_tables_names) == 8

        assert output_tables_names[0].table == "output1_select"
        assert output_tables_names[0].kind == TableKind.DQ_SELECT
        assert output_tables_names[1].table == "output1_filter"
        assert output_tables_names[1].kind == TableKind.DQ_FILTER
        assert output_tables_names[2].table == "output1_summary"
        assert output_tables_names[2].kind == TableKind.DQ_SUMMARY

        assert output_tables_names[3].table == "output2_select"
        assert output_tables_names[3].kind == TableKind.DQ_SELECT
        assert output_tables_names[4].table == "output2_filter"
        assert output_tables_names[4].kind == TableKind.DQ_FILTER
        assert output_tables_names[5].table == "output2_dq_summary"
        assert output_tables_names[5].kind == TableKind.DQ_SUMMARY

        assert output_tables_names[6].table == "output3_select"
        assert output_tables_names[6].kind == TableKind.DQ_SELECT
        assert output_tables_names[7].table == "output3_summary"
        assert output_tables_names[7].kind == TableKind.DQ_SUMMARY

        assert input_tables_names == []

    def test_table_names_many_input_and_output_actions(self):

        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "raw_data1",
                    None,
                ),
                0,
            ),
            "input2": ExtendedTableURI(
                TableURI(
                    None,
                    "raw_data2",
                    None,
                ),
                1,
            ),
        }
        output_tables = ["output1", "output2"]

        input_action1 = DataQuality(
            "input1",
            classifiers=[],
            operations=[
                Select(to_table="input1_select"),
                Filter(),
                Summary(table="input1_summary"),
            ],
        )

        input_action2 = DataQuality(
            "input2",
            classifiers=[],
            operations=[
                Select(to_table="input2_select"),
                Filter(to_table="input2_filter"),
                Summary(table=None),
            ],
        )

        output_action1 = DataQuality(
            "output1",
            classifiers=[],
            operations=[
                Select(to_table="output1_select"),
                Filter(to_table="output1_filter"),
            ],
        )

        output_action2 = DataQuality(
            "output2",
            classifiers=[],
            operations=[
                Summary(table="output2_summary"),
            ],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=output_tables,
            input_actions=[input_action1, input_action2],
            output_actions=[output_action1, output_action2],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 5
        assert input_tables_names[0].table == "input1_select"
        assert input_tables_names[1].table == "input1_summary"
        assert input_tables_names[2].table == "input2_select"
        assert input_tables_names[3].table == "input2_filter"
        assert input_tables_names[4].table == "raw_data2_dq_summary"

        assert len(output_tables_names) == 3
        assert output_tables_names[0].table == "output1_select"
        assert output_tables_names[1].table == "output1_filter"
        assert output_tables_names[2].table == "output2_summary"

    def test_table_names_many_operations_single_action_all_custom(self):
        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "master_table",
                    None,
                ),
                0,
            )
        }

        dq_action = DataQuality(
            "input1",
            classifiers=[],
            operations=[
                Select(to_table="custom_select_1"),
                Select(to_table="custom_select_2"),
                Filter(to_table="custom_filter_1"),
                Filter(to_table="custom_filter_2"),
                Summary(table="custom_summary_1"),
                Summary(table="custom_summary_2"),
            ],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 6

        assert input_tables_names[0].table == "custom_select_1"
        assert input_tables_names[1].table == "custom_select_2"
        assert input_tables_names[2].table == "custom_filter_1"
        assert input_tables_names[3].table == "custom_filter_2"
        assert input_tables_names[4].table == "custom_summary_1"
        assert input_tables_names[5].table == "custom_summary_2"

        assert output_tables_names == []

    def test_table_names_five_actions_each_with_five_operations(self):

        input_tables = {
            f"input{i}": ExtendedTableURI(
                TableURI(
                    None,
                    f"table_{i}",
                    None,
                ),
                i - 1,
            )
            for i in range(1, 6)
        }

        actions = []
        for i in range(1, 6):
            action = DataQuality(
                f"input{i}",
                classifiers=[],
                operations=[
                    Select(to_table=f"select_{i}_1"),
                    Select(to_table=f"select_{i}_2"),
                    Filter(to_table=f"filter_{i}_1"),
                    Filter(),
                    Summary(table=f"summary_{i}_1"),
                ],
            )
            actions.append(action)

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=actions,
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 20

        assert input_tables_names[0].table == "select_1_1"
        assert input_tables_names[1].table == "select_1_2"
        assert input_tables_names[2].table == "filter_1_1"
        assert input_tables_names[3].table == "summary_1_1"

        assert input_tables_names[16].table == "select_5_1"
        assert input_tables_names[17].table == "select_5_2"
        assert input_tables_names[18].table == "filter_5_1"
        assert input_tables_names[19].table == "summary_5_1"

        assert output_tables_names == []

    def test_table_names_complex_real_world_scenario(self):
        input_tables = {
            "raw_sales": ExtendedTableURI(
                TableURI(
                    "sales_db",
                    "sales_transactions",
                    "HEAD",
                ),
                0,
            ),
            "raw_inventory": ExtendedTableURI(
                TableURI(
                    "inventory_db",
                    "inventory_levels",
                    "HEAD~1",
                ),
                1,
            ),
            "raw_customers": ExtendedTableURI(
                TableURI(
                    None,
                    "customer_records",
                    None,
                ),
                2,
            ),
            "raw_products": ExtendedTableURI(
                TableURI(
                    None,
                    "product_catalog",
                    "HEAD^^",
                ),
                3,
            ),
        }

        output_tables = [
            "cleaned_sales",
            "cleaned_inventory",
            "cleaned_customers",
        ]

        input_action1 = DataQuality(
            "raw_sales",
            classifiers=[],
            operations=[
                Select(to_table="valid_sales"),
                Filter(to_table="invalid_sales"),
                Summary(table=None),
            ],
        )

        input_action2 = DataQuality(
            "raw_inventory",
            classifiers=[],
            operations=[
                Select(to_table="inventory_select"),
                Filter(),
                Summary(table="inventory_summary_report"),
            ],
        )

        input_action3 = DataQuality(
            "raw_customers",
            classifiers=[],
            operations=[
                Select(to_table="verified_customers"),
                Summary(table=None),
            ],
        )

        input_action4 = DataQuality(
            "raw_products",
            classifiers=[],
            operations=[
                Filter(),
                Summary(table="product_quality_report"),
            ],
        )

        output_action1 = DataQuality(
            "cleaned_sales",
            classifiers=[],
            operations=[
                Select(to_table="final_sales"),
                Summary(table=None),
            ],
        )

        output_action2 = DataQuality(
            "cleaned_inventory",
            classifiers=[],
            operations=[
                Filter(),
            ],
        )

        output_action3 = DataQuality(
            "cleaned_customers",
            classifiers=[],
            operations=[
                Summary(table="customer_quality_metrics"),
            ],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=output_tables,
            input_actions=[input_action1, input_action2, input_action3, input_action4],
            output_actions=[output_action1, output_action2, output_action3],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 8

        assert input_tables_names[0].table == "valid_sales"
        assert input_tables_names[1].table == "invalid_sales"
        assert input_tables_names[2].table == "sales_transactions_dq_summary"

        assert input_tables_names[3].table == "inventory_select"
        assert input_tables_names[4].table == "inventory_summary_report"

        assert input_tables_names[5].table == "verified_customers"
        assert input_tables_names[6].table == "customer_records_dq_summary"

        assert input_tables_names[7].table == "product_quality_report"

        assert len(output_tables_names) == 3

        assert output_tables_names[0].table == "final_sales"
        assert output_tables_names[1].table == "cleaned_sales_dq_summary"
        assert output_tables_names[2].table == "customer_quality_metrics"

    def test_duplicate_validation_same_custom_name_in_input_actions(self):
        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "source_table",
                    None,
                ),
                0,
            )
        }

        dq_action = DataQuality(
            "input1",
            classifiers=[],
            operations=[
                Select(to_table="duplicate_name"),
                Filter(to_table="duplicate_name"),
            ],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        with pytest.raises(DataQualityProcessingError) as exception:
            processor.table_names()

        assert exception.value.code == "DQPE-004"
        assert "duplicate_name" in str(exception.value)
        assert "appears 2 times" in str(exception.value)

    def test_select_rejects_none_to_table(self):

        with pytest.raises(DataQualityConfigurationError):
            # noinspection PyTypeChecker
            Select(to_table=None)

    def test_filter_and_select_behavior_with_to_table(self):

        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "source_table",
                    None,
                ),
                0,
            )
        }

        dq_action_filter_with_table = DataQuality(
            "input1",
            classifiers=[],
            operations=[Filter(to_table="filtered_output")],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action_filter_with_table],
            output_actions=[],
        )

        input_tables_names, _ = processor.table_names()
        assert len(input_tables_names) == 1
        assert input_tables_names[0].table == "filtered_output"
        assert input_tables_names[0].kind == TableKind.DQ_FILTER

        dq_action_filter_no_table = DataQuality(
            "input1",
            classifiers=[],
            operations=[Filter()],
        )

        processor2 = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action_filter_no_table],
            output_actions=[],
        )

        input_tables_names2, _ = processor2.table_names()
        assert len(input_tables_names2) == 0

        dq_action_select = DataQuality(
            "input1",
            classifiers=[],
            operations=[Select(to_table="selected_output")],
        )

        processor3 = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action_select],
            output_actions=[],
        )

        input_tables_names3, _ = processor3.table_names()
        assert len(input_tables_names3) == 1
        assert input_tables_names3[0].table == "selected_output"
        assert input_tables_names3[0].kind == TableKind.DQ_SELECT

    def test_duplicate_validation_same_custom_name_across_input_actions(self):
        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "table1",
                    None,
                ),
                0,
            ),
            "input2": ExtendedTableURI(
                TableURI(
                    None,
                    "table2",
                    None,
                ),
                1,
            ),
        }

        dq_action1 = DataQuality(
            "input1",
            classifiers=[],
            operations=[
                Select(to_table="shared_output"),
            ],
        )

        dq_action2 = DataQuality(
            "input2",
            classifiers=[],
            operations=[
                Filter(to_table="shared_output"),
            ],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action1, dq_action2],
            output_actions=[],
        )

        with pytest.raises(DataQualityProcessingError) as exception:
            processor.table_names()

        assert exception.value.code == "DQPE-004"
        assert "shared_output" in str(exception.value)

    def test_duplicate_validation_same_custom_name_in_output_actions(self):
        output_tables = ["output1"]

        dq_action = DataQuality(
            "output1",
            classifiers=[],
            operations=[
                Select(to_table="duplicate_output"),
                Summary(table="duplicate_output"),
            ],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables={},
            output_tables=output_tables,
            input_actions=[],
            output_actions=[dq_action],
        )

        with pytest.raises(DataQualityProcessingError) as exception:
            processor.table_names()

        assert exception.value.code == "DQPE-004"
        assert "duplicate_output" in str(exception.value)

    def test_duplicate_validation_same_custom_name_across_output_actions(self):
        output_tables = ["output1", "output2"]

        dq_action1 = DataQuality(
            "output1",
            classifiers=[],
            operations=[
                Select(to_table="shared_name"),
            ],
        )

        dq_action2 = DataQuality(
            "output2",
            classifiers=[],
            operations=[
                Filter(to_table="shared_name"),
            ],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables={},
            output_tables=output_tables,
            input_actions=[],
            output_actions=[dq_action1, dq_action2],
        )

        with pytest.raises(DataQualityProcessingError) as exception:
            processor.table_names()

        assert exception.value.code == "DQPE-004"
        assert "shared_name" in str(exception.value)

    def test_duplicate_validation_same_name_across_input_and_output(self):
        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "source",
                    None,
                ),
                0,
            )
        }
        output_tables = ["output1"]

        input_action = DataQuality(
            "input1",
            classifiers=[],
            operations=[
                Select(to_table="conflict_name"),
            ],
        )

        output_action = DataQuality(
            "output1",
            classifiers=[],
            operations=[
                Filter(to_table="conflict_name"),
            ],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=output_tables,
            input_actions=[input_action],
            output_actions=[output_action],
        )

        with pytest.raises(DataQualityProcessingError) as exception:
            processor.table_names()

        assert exception.value.code == "DQPE-004"
        assert "conflict_name" in str(exception.value)

    def test_duplicate_validation_multiple_duplicates(self):
        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "source",
                    None,
                ),
                0,
            )
        }

        dq_action = DataQuality(
            "input1",
            classifiers=[],
            operations=[
                Select(to_table="dup1"),
                Select(to_table="dup1"),
                Filter(to_table="dup2"),
                Filter(to_table="dup2"),
            ],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        with pytest.raises(DataQualityProcessingError) as exception:
            processor.table_names()

        assert exception.value.code == "DQPE-004"
        assert "dup1" in str(exception.value)
        assert "dup2" in str(exception.value)
        assert "appears 2 times" in str(exception.value)

    def test_duplicate_validation_triple_duplicate(self):
        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "source",
                    None,
                ),
                0,
            )
        }

        dq_action = DataQuality(
            "input1",
            classifiers=[],
            operations=[
                Select(to_table="triple"),
                Filter(to_table="triple"),
                Summary(table="triple"),
            ],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action],
            output_actions=[],
        )

        with pytest.raises(DataQualityProcessingError) as exception:
            processor.table_names()

        assert exception.value.code == "DQPE-004"
        assert "triple" in str(exception.value)
        assert "appears 3 times" in str(exception.value)

    def test_no_duplicate_validation_unique_names(self):
        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "source",
                    None,
                ),
                0,
            )
        }
        output_tables = ["output1"]

        input_action = DataQuality(
            "input1",
            classifiers=[],
            operations=[
                Select(to_table="unique_select"),
                Filter(to_table="unique_filter"),
                Summary(table="unique_summary"),
            ],
        )

        output_action = DataQuality(
            "output1",
            classifiers=[],
            operations=[
                Select(to_table="unique_output_select"),
                Summary(table="unique_output_summary"),
            ],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=output_tables,
            input_actions=[input_action],
            output_actions=[output_action],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 3
        assert len(output_tables_names) == 2

    def test_no_duplicate_validation_unique_names_from_different_sources(self):

        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "source1",
                    None,
                ),
                0,
            ),
            "input2": ExtendedTableURI(
                TableURI(
                    None,
                    "source2",
                    None,
                ),
                1,
            ),
        }

        dq_action1 = DataQuality(
            "input1",
            classifiers=[],
            operations=[
                Select(to_table="source1_select"),
            ],
        )

        dq_action2 = DataQuality(
            "input2",
            classifiers=[],
            operations=[
                Select(to_table="source2_select"),
            ],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action1, dq_action2],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 2
        assert input_tables_names[0].table == "source1_select"
        assert input_tables_names[1].table == "source2_select"

    def test_duplicate_validation_complex_scenario(self):
        input_tables = {
            "input1": ExtendedTableURI(
                TableURI(
                    None,
                    "raw_data1",
                    None,
                ),
                0,
            ),
            "input2": ExtendedTableURI(
                TableURI(
                    None,
                    "raw_data2",
                    None,
                ),
                1,
            ),
        }
        output_tables = ["output1", "output2"]

        input_action1 = DataQuality(
            "input1",
            classifiers=[],
            operations=[
                Select(to_table="processed_data"),
                Filter(to_table=None),
            ],
        )

        input_action2 = DataQuality(
            "input2",
            classifiers=[],
            operations=[
                Summary(table="processed_data"),
            ],
        )

        output_action1 = DataQuality(
            "output1",
            classifiers=[],
            operations=[
                Select(to_table="final_output"),
            ],
        )

        processor = DataQualityProcessor(
            collection="collection",
            input_tables=input_tables,
            output_tables=output_tables,
            input_actions=[input_action1, input_action2],
            output_actions=[output_action1],
        )

        with pytest.raises(DataQualityProcessingError) as exception:
            processor.table_names()

        assert exception.value.code == "DQPE-004"
        assert "processed_data" in str(exception.value)

    def test_from_table_and_from_collection_with_different_collections(self):
        input_tables = {
            "table_a": ExtendedTableURI(
                TableURI(
                    "collection_a",
                    "source_table_a",
                    None,
                ),
                0,
            ),
            "table_b": ExtendedTableURI(
                TableURI(
                    "collection_b",
                    "source_table_b",
                    None,
                ),
                1,
            ),
            "table_c": ExtendedTableURI(
                TableURI(
                    None,
                    "source_table_c",
                    None,
                ),
                2,
            ),
        }

        dq_action1 = DataQuality(
            "table_a",
            classifiers=[],
            operations=[
                Select(to_table="output_from_a"),
                Filter(to_table="filtered_from_a"),
            ],
        )

        dq_action2 = DataQuality(
            "table_b",
            classifiers=[],
            operations=[
                Summary(table="summary_from_b"),
            ],
        )

        dq_action3 = DataQuality(
            "table_c",
            classifiers=[],
            operations=[
                Select(to_table="output_from_c"),
            ],
        )

        processor = DataQualityProcessor(
            collection="default_collection",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action1, dq_action2, dq_action3],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 4

        assert input_tables_names[0].table == "output_from_a"
        assert input_tables_names[0].from_table == "source_table_a"
        assert input_tables_names[0].from_collection == "collection_a"
        assert input_tables_names[0].collection == "default_collection"

        assert input_tables_names[1].table == "filtered_from_a"
        assert input_tables_names[1].from_table == "source_table_a"
        assert input_tables_names[1].from_collection == "collection_a"
        assert input_tables_names[1].collection == "default_collection"

        assert input_tables_names[2].table == "summary_from_b"
        assert input_tables_names[2].from_table == "source_table_b"
        assert input_tables_names[2].from_collection == "collection_b"
        assert input_tables_names[2].collection == "default_collection"

        assert input_tables_names[3].table == "output_from_c"
        assert input_tables_names[3].from_table == "source_table_c"
        assert input_tables_names[3].from_collection == "default_collection"
        assert input_tables_names[3].collection == "default_collection"

    def test_from_table_and_from_collection_output_actions(self):
        output_tables = ["output_table1", "output_table2"]

        dq_action1 = DataQuality(
            "output_table1",
            classifiers=[],
            operations=[
                Select(to_table="validated_output1"),
                Summary(table="summary_output1"),
            ],
        )

        dq_action2 = DataQuality(
            "output_table2",
            classifiers=[],
            operations=[
                Filter(to_table="filtered_output2"),
            ],
        )

        processor = DataQualityProcessor(
            collection="my_collection",
            input_tables={},
            output_tables=output_tables,
            input_actions=[],
            output_actions=[dq_action1, dq_action2],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(output_tables_names) == 3

        assert output_tables_names[0].table == "validated_output1"
        assert output_tables_names[0].from_table == "output_table1"
        assert output_tables_names[0].from_collection == "my_collection"
        assert output_tables_names[0].kind == TableKind.DQ_SELECT

        assert output_tables_names[1].table == "summary_output1"
        assert output_tables_names[1].from_table == "output_table1"
        assert output_tables_names[1].from_collection == "my_collection"
        assert output_tables_names[1].kind == TableKind.DQ_SUMMARY

        assert output_tables_names[2].table == "filtered_output2"
        assert output_tables_names[2].from_table == "output_table2"
        assert output_tables_names[2].from_collection == "my_collection"
        assert output_tables_names[2].kind == TableKind.DQ_FILTER

    def test_from_table_and_from_collection_with_qualified_table_reference(self):
        input_tables = {
            "alias1": ExtendedTableURI(
                TableURI(
                    "sales",
                    "transactions",
                    "HEAD",
                ),
                0,
            ),
            "alias2": ExtendedTableURI(
                TableURI(
                    "inventory",
                    "stock_levels",
                    None,
                ),
                1,
            ),
        }

        dq_action1 = DataQuality(
            "sales/transactions@HEAD",
            classifiers=[],
            operations=[
                Select(to_table="clean_transactions"),
            ],
        )

        dq_action2 = DataQuality(
            "inventory/stock_levels",
            classifiers=[],
            operations=[
                Filter(to_table="valid_stock"),
            ],
        )

        processor = DataQualityProcessor(
            collection="analytics",
            input_tables=input_tables,
            output_tables=[],
            input_actions=[dq_action1, dq_action2],
            output_actions=[],
        )

        input_tables_names, output_tables_names = processor.table_names()

        assert len(input_tables_names) == 2

        assert input_tables_names[0].table == "clean_transactions"
        assert input_tables_names[0].from_table == "transactions"
        assert input_tables_names[0].from_collection == "sales"
        assert input_tables_names[0].collection == "analytics"

        assert input_tables_names[1].table == "valid_stock"
        assert input_tables_names[1].from_table == "stock_levels"
        assert input_tables_names[1].from_collection == "inventory"
        assert input_tables_names[1].collection == "analytics"
