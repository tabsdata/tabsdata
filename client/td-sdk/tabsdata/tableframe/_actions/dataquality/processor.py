#
# Copyright 2025 Tabs Data Inc.
#
import logging
from typing import TYPE_CHECKING, Tuple, cast

from tabsdata._table_actions.actions import OnTablesAction
from tabsdata._tableinfo import TableInfo, TableKind
from tabsdata._tableuri import ExtendedTableURI, build_table_uri_object
from tabsdata._tabsserver.function.typing import (
    DerivedTableFrame,
    DerivedTableFrameStatus,
    InputTableSpec,
    OutputTableSpec,
)
from tabsdata.dataquality import DataQuality, Filter, Select, Summary
from tabsdata.dataquality._operations import Operation
from tabsdata.exceptions import DataQualityProcessingError, ErrorCode
from tabsdata.tableframe._actions.dataquality.engine import (
    ClassifierFamily,
    DataQualityEngine,
)
from tabsdata.tableframe.functions.eager import concat
from tabsdata.tableframe.lazyframe.frame import TableFrame

if TYPE_CHECKING:
    from tabsdata._tabsserver.function.execution_context import ExecutionContext

logger = logging.getLogger(__name__)


class ExtendedTable:
    def __init__(self, table: str, position: int):
        self.table = table
        self.position = position

    def __hash__(self) -> int:
        return hash(self.table)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ExtendedTable):
            return False
        return self.table == other.table

    @property
    def table(self) -> str:
        return self._table

    @table.setter
    def table(self, table: str):
        self._table = table

    @property
    def position(self) -> int:
        return self._position

    @position.setter
    def position(self, position: int):
        self._position = position


class DataQualityProcessor:
    def __init__(
        self,
        collection: str,
        input_tables: dict[str, ExtendedTableURI],
        output_tables: list[str],
        input_actions: list[OnTablesAction],
        output_actions: list[OnTablesAction],
    ):
        self._collection: str = collection
        self._input_tables: dict[str, ExtendedTableURI] = input_tables
        self.output_tables = output_tables
        self.input_actions = input_actions
        self.output_actions = output_actions

    @property
    def collection(self) -> str:
        return self._collection

    @collection.setter
    def collection(self, collection: str):
        self._collection = collection

    @property
    def input_tables(self) -> dict[str, ExtendedTableURI]:
        return self._input_tables

    @input_tables.setter
    def input_tables(self, input_tables: dict[str, ExtendedTableURI]):
        self._input_tables = input_tables

    @property
    def input_tables_map(self) -> dict[tuple[str, str], list[ExtendedTableURI]]:
        input_tables_map: dict[tuple[str, str], list[ExtendedTableURI]] = {}
        for input_table in self.input_tables.values():
            collection = (
                input_table.collection if input_table.collection else self.collection
            )
            table = input_table
            version = input_table.version

            qualified_table = (collection, input_table.table)

            if qualified_table not in input_tables_map:
                input_tables_map[qualified_table] = []

            for table_uri in input_tables_map[qualified_table]:
                table_uri_collection = (
                    table_uri.collection if table_uri.collection else self.collection
                )
                table_uri_table = table_uri.table
                table_uri_version = table_uri.version
                if (
                    collection == table_uri_collection
                    and table == table_uri_table
                    and version == table_uri_version
                ):
                    break
            else:
                input_tables_map[qualified_table].append(input_table)
        return input_tables_map

    @property
    def output_tables(self) -> dict[str, ExtendedTable]:
        return self._output_tables

    @output_tables.setter
    def output_tables(self, output_tables: list[str]):
        self._output_tables = {
            table: ExtendedTable(table, position)
            for position, table in enumerate(output_tables)
        }

    @property
    def input_actions(self) -> list[DataQuality]:
        return self._input_actions

    @input_actions.setter
    def input_actions(self, input_actions: list[OnTablesAction]):
        self._input_actions = self.filter_dataquality_actions(input_actions)

    @property
    def output_actions(self) -> list[DataQuality]:
        return self._output_actions

    @output_actions.setter
    def output_actions(self, output_actions: list[OnTablesAction]):
        self._output_actions = self.filter_dataquality_actions(output_actions)

    @classmethod
    def from_execution_context(
        cls,
        context: "ExecutionContext",
    ) -> "DataQualityProcessor":
        from tabsdata._io.inputs.table_inputs import TableInput
        from tabsdata._io.outputs.table_outputs import TableOutput

        input_tables = (
            cast(TableInput, context.source)._extended_table_uris_map
            if isinstance(context.source, TableInput)
            else {}
        )

        output_tables = (
            cast(TableOutput, context.destination)._table_list
            if isinstance(context.destination, TableOutput)
            else []
        )

        try:
            # noinspection PyUnresolvedReferences
            input_actions = context.user_provided_function.input_actions
        except AttributeError:
            input_actions = []

        try:
            output_actions = context.user_provided_function.output_actions
        except AttributeError:
            output_actions = []

        return cls(
            collection=context.request.collection,
            input_tables=input_tables,
            output_tables=output_tables,
            input_actions=input_actions,
            output_actions=output_actions,
        )

    @classmethod
    def filter_dataquality_actions(
        cls,
        actions: list[OnTablesAction],
    ) -> list[DataQuality]:
        return [action for action in actions or [] if isinstance(action, DataQuality)]

    def table_names(self) -> tuple[list[TableInfo], list[TableInfo]]:
        input_actions_tables = []
        for action in self._input_actions:
            collection, table, position = self.resolve_input_table(action.table)
            input_actions_tables.extend(
                self.build_table_names(collection, table, position, action.operations)
            )

        output_actions_tables = []
        for action in self._output_actions:
            collection, table, position = self.resolve_output_table(action.table)
            output_actions_tables.extend(
                self.build_table_names(collection, table, position, action.operations)
            )

        self.validate_no_duplicate_table_names(
            input_actions_tables, output_actions_tables
        )

        return input_actions_tables, output_actions_tables

    def resolve_input_table(self, action_table: str) -> tuple[str, str, int]:
        if action_table in self.input_tables:
            table_uri = self.input_tables[action_table]
            return (
                self.collection if not table_uri.collection else table_uri.collection,
                table_uri.table,
                table_uri.position,
            )

        table_uri = build_table_uri_object(action_table)
        collection = table_uri.collection if table_uri.collection else self.collection
        table = table_uri.table

        qualified_table = (collection, table)
        if qualified_table in self.input_tables_map:
            table_uris = self.input_tables_map[qualified_table]
            if len(table_uris) == 1:
                return collection, table_uris[0].table, table_uris[0].position

        raise DataQualityProcessingError(ErrorCode.DQPE1, action_table)

    def resolve_output_table(self, action_table: str) -> tuple[str, str, int]:
        if action_table in self.output_tables:
            return (
                self.collection,
                action_table,
                self.output_tables[action_table].position,
            )

        raise DataQualityProcessingError(ErrorCode.DQPE2, action_table)

    def build_table_names(
        self,
        collection: str,
        table: str,
        position: int | None,
        operations: list[Operation],
    ) -> list[TableInfo]:
        table_names = []
        if operations:
            for operation in operations:
                match operation:
                    case Filter():
                        if operation.to_table is not None:
                            table_name = operation.to_table
                            table_names.append(
                                self.table_name_for_filter(
                                    self._collection,
                                    table_name,
                                    position,
                                    collection,
                                    table,
                                )
                            )
                    case Select():
                        table_name = operation.to_table
                        table_names.append(
                            self.table_name_for_select(
                                self._collection,
                                table_name,
                                position,
                                collection,
                                table,
                            )
                        )
                    case Summary():
                        has_custom_table_name = operation.table is not None
                        table_name = operation.table if has_custom_table_name else table
                        table_names.append(
                            self.table_name_for_summary(
                                self._collection,
                                table_name,
                                position,
                                has_custom_table_name,
                                collection,
                                table,
                            )
                        )
                    case _:
                        raise DataQualityProcessingError(
                            ErrorCode.DQPE3, type(operation)
                        )
        return table_names

    @classmethod
    def table_name_for_filter(
        cls,
        collection: str,
        table: str,
        position: int,
        from_collection: str,
        from_table: str,
    ) -> TableInfo:
        return cls.derived_table_name(
            collection=collection,
            table=table,
            position=position,
            kind=TableKind.DQ_FILTER,
            is_custom=True,
            from_table=from_table,
            from_collection=from_collection,
        )

    @classmethod
    def table_name_for_select(
        cls,
        collection: str,
        table: str,
        position: int,
        from_collection: str,
        from_table: str,
    ) -> TableInfo:
        return cls.derived_table_name(
            collection=collection,
            table=table,
            position=position,
            kind=TableKind.DQ_SELECT,
            is_custom=True,
            from_table=from_table,
            from_collection=from_collection,
        )

    @classmethod
    def table_name_for_summary(
        cls,
        collection: str,
        table: str,
        position: int,
        is_custom: bool,
        from_collection: str,
        from_table: str,
    ) -> TableInfo:
        return cls.derived_table_name(
            collection=collection,
            table=table,
            position=position,
            kind=TableKind.DQ_SUMMARY,
            is_custom=is_custom,
            from_table=from_table,
            from_collection=from_collection,
        )

    @classmethod
    def derived_table_name(
        cls,
        collection: str,
        table: str,
        position: int,
        kind: TableKind,
        is_custom: bool,
        from_collection: str,
        from_table: str,
    ) -> TableInfo:
        suffix = "" if (is_custom or kind.suffix is None) else kind.suffix
        return TableInfo(
            from_table=from_table,
            from_collection=from_collection,
            kind=kind,
            table=table + suffix,
            collection=collection,
            position=position,
        )

    @classmethod
    def validate_no_duplicate_table_names(
        cls,
        input_tables: list[TableInfo],
        output_tables: list[TableInfo],
    ) -> None:
        all_tables = input_tables + output_tables
        table_map: dict[tuple[str, str], list[int]] = {}
        for idx, table_info in enumerate(all_tables):
            key = (table_info.collection, table_info.table)
            if key not in table_map:
                table_map[key] = []
            table_map[key].append(idx)
        duplicates = {
            key: indices for key, indices in table_map.items() if len(indices) > 1
        }
        if duplicates:
            duplicates_report = []
            for (collection, table), indices in duplicates.items():
                count = len(indices)
                duplicates_report.append(
                    f"  - Table '{collection}/{table}' appears {count} times"
                )
            raise DataQualityProcessingError(
                ErrorCode.DQPE4, "\n".join(duplicates_report)
            )

    # noinspection DuplicatedCode
    def tables_data(
        self,
        input_table_frames: InputTableSpec,
        output_table_frames: OutputTableSpec,
        derived_table_frames: OutputTableSpec,
    ):
        for action in self.input_actions:
            collection, table, position = self.resolve_input_table(action.table)
            table_infos = self.build_table_names(
                collection, table, position, action.operations
            )
            if position >= len(input_table_frames):
                raise DataQualityProcessingError(
                    ErrorCode.DQPE5, position, action.table, len(input_table_frames)
                )
            tableframe = input_table_frames[position]
            derived_table_frame, dq_cols, dq_tags = self.compute(tableframe, action)
            if len(table_infos) != len(action.operations):
                raise DataQualityProcessingError(
                    ErrorCode.DQPE7,
                    action.table,
                    len(table_infos),
                    len(action.operations),
                )
            for table_info, operation in zip(table_infos, action.operations):
                derived_table_frames.append(
                    self.harvest(
                        derived_table_frame, dq_cols, dq_tags, table_info, operation
                    )
                )

        for action in self.output_actions:
            collection, table, position = self.resolve_output_table(action.table)
            table_infos = self.build_table_names(
                collection, table, position, action.operations
            )
            if position >= len(output_table_frames):
                raise DataQualityProcessingError(
                    ErrorCode.DQPE6, position, action.table, len(output_table_frames)
                )
            tableframe = output_table_frames[position]
            derived_table_frame, dq_cols, dq_tags = self.compute(tableframe, action)
            if len(table_infos) != len(action.operations):
                raise DataQualityProcessingError(
                    ErrorCode.DQPE8,
                    action.table,
                    len(table_infos),
                    len(action.operations),
                )
            for table_info, operation in zip(table_infos, action.operations):
                derived_table_frames.append(
                    self.harvest(
                        derived_table_frame, dq_cols, dq_tags, table_info, operation
                    )
                )

    @classmethod
    def to_tableframe(
        cls,
        tableframes: InputTableSpec | OutputTableSpec | None,
    ) -> TableFrame | None:
        tf: list[TableFrame] = []
        match tableframes:
            case None:
                pass
            case TableFrame():
                tf.append(tableframes)
            case list():
                for tableframe in tableframes:
                    if tableframe is None:
                        continue
                    elif isinstance(tableframe, TableFrame):
                        tf.append(tableframe)
                    elif isinstance(tableframe, list):
                        for sub_tableframe in tableframe:
                            if isinstance(sub_tableframe, TableFrame):
                                tf.append(sub_tableframe)
        if not tf:
            return None
        return concat(tf, how="diagonal_relaxed")

    @classmethod
    def compute(
        cls,
        tableframe: InputTableSpec | OutputTableSpec | None,
        action: DataQuality,
    ) -> Tuple[
        DerivedTableFrame | None,
        dict[str, ClassifierFamily] | None,
        dict[str, set[str]] | None,
    ]:
        tf = cls.to_tableframe(tableframe)
        if tf is None:
            return None, None, None

        dtf: DerivedTableFrame = DerivedTableFrame(
            DerivedTableFrameStatus.INITIALIZED, tableframe, tf
        )
        dq = DataQualityEngine(tf)
        classifiers = (
            action.classifiers
            if isinstance(action.classifiers, list)
            else [action.classifiers]
        )
        for classifier in classifiers:
            dq = dq._classify(classifier=classifier)

        dtf.derived_tf = dq.tf()
        dtf.status = DerivedTableFrameStatus.COMPUTED
        return dtf, dq._cols, dq._tags

    @classmethod
    def harvest(
        cls,
        tableframe: DerivedTableFrame | None,
        cols: dict[str, ClassifierFamily],
        tags: dict[str, set[str]] | None,
        _table_info: TableInfo,
        operation: Operation,
    ) -> DerivedTableFrame | None:
        if tableframe is None:
            return None
        tf = tableframe.derived_tf
        if tf is None:
            return None

        dtf: DerivedTableFrame = DerivedTableFrame(
            tableframe.status, tableframe.original_tf, None
        )
        dq = DataQualityEngine(tf, cols, tags)
        dq = dq.summary(operation.tags)
        dtf.derived_tf = dq.tf()
        dtf.status = DerivedTableFrameStatus.HARVESTED
        return dtf
