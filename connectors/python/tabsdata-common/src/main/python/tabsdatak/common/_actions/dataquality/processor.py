#
# Copyright 2026 Tabsdata Inc.
#

import logging
from tempfile import TemporaryDirectory
from typing import Callable, Dict, NamedTuple, Tuple, TypeAlias

# noinspection PyProtectedMember
import tabsdatak.tableframe.schema as td_typing
from tabsdatak._info import TransformerInfo
from tabsdatak.api import OnTablesAction
from tabsdatak.common._actions.dataquality.engine import (
    ColumnInfo,
    DataQualityEngine,
)
from tabsdatak.common._actions.dataquality.record import (
    DerivedTableFrame,
    DerivedTableFrameStatus,
    Harvest,
)
from tabsdatak.common._actions.dataquality.trend import build_trend_tfr_mod
from tabsdatak.dataquality import DataQuality, Filter, Select, Summary
from tabsdatak.dataquality._error import DataQualityErrorCode
from tabsdatak.dataquality._operator import Enrich, Fail, Operator
from tabsdatak.dataquality._tableinfo import TableInfo, TableKind
from tabsdatak.dataquality._tableuri import ExtendedTableURI, build_table_uri_object
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import concat

SimpleTableSpec: TypeAlias = "TableFrame | None"
InputTableSpec: TypeAlias = list
OutputTableSpec: TypeAlias = list
DerivedTableFrameSpec: TypeAlias = list

logger = logging.getLogger(__name__)


class TrendTransformerInfo(NamedTuple):
    """A trend transformer paired with the directory holding its module.

    ``project_root`` is the temporary directory holding the module that implements the
    trend tfr.
    """

    info: TransformerInfo
    project_root: TemporaryDirectory


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
    def from_actions(
        cls,
        *,
        collection: str,
        output_tables: list[str],
        actions: list,
    ) -> "DataQualityProcessor":
        return cls(
            collection=collection,
            input_tables={},
            output_tables=output_tables,
            input_actions=[],
            output_actions=actions,
        )

    @classmethod
    def filter_dataquality_actions(
        cls,
        actions: list[OnTablesAction],
    ) -> list[DataQuality]:
        return [action for action in actions or [] if isinstance(action, DataQuality)]

    def table_names(self) -> tuple[list[TableInfo], list[TableInfo]]:
        overridden_input_actions_tables: Dict[str, int] = {}
        input_actions_tables: list[TableInfo] = []
        for action in self._input_actions:
            collection, table, position = self.resolve_input_table(action.table)
            input_actions_tables.extend(
                self.build_table_names(collection, table, position, action.operators)
            )
            for operator in action.operators:
                if self.overriding(operator):
                    overridden_input_actions_tables[table] = (
                        overridden_input_actions_tables.get(table, 0) + 1
                    )

        overridden_output_actions_tables: Dict[str, int] = {}
        output_actions_tables: list[TableInfo] = []
        for action in self._output_actions:
            collection, table, position = self.resolve_output_table(action.table)
            output_actions_tables.extend(
                self.build_table_names(collection, table, position, action.operators)
            )
            for operator in action.operators:
                if self.overriding(operator):
                    overridden_output_actions_tables[table] = (
                        overridden_output_actions_tables.get(table, 0) + 1
                    )

        self.validate_no_multiple_times_overridden_table_names(
            overridden_input_actions_tables, overridden_output_actions_tables
        )

        self.validate_no_duplicate_table_names(
            input_actions_tables, output_actions_tables + self.trend_table_names()
        )

        return input_actions_tables, output_actions_tables

    def trend_table_names(self) -> list[TableInfo]:
        """The trend tables the output actions' ``Summary`` operators derive.

        Kept apart from :meth:`table_names` because a trend table is the output
        of the system transformer that maintains it, not of the function whose
        summaries it reads -- it is registered with that transformer, not here.
        """
        names: list[TableInfo] = []
        for action in self._output_actions:
            collection, table, position = self.resolve_output_table(action.table)
            for operator in action.operators or []:
                if not isinstance(operator, Summary):
                    continue
                trend = operator.trend
                if trend is None:
                    continue
                names.append(
                    self.table_name_for_trends(
                        self._collection,
                        trend.table if trend.table is not None else table,
                        position,
                        trend.table is not None,
                        collection,
                        table,
                    )
                )
        return names

    def build_trend_transformers_info(self) -> list[TrendTransformerInfo]:
        """Build the system transformers that maintain the DQ summary trend tables.

        One :class:`TransformerInfo` per ``Summary`` operator declared on an
        output action. Each reads that operator's summary table plus its own
        previous version, and writes the trend table named by
        ``SummaryTrend.table``, falling back to ``<target>_dq_summary_trends``
        when it is ``None``. A ``Summary`` whose ``trend`` is ``None``
        contributes nothing.
        """
        transformers: list[TrendTransformerInfo] = []
        for action in self._output_actions:
            collection, table, position = self.resolve_output_table(action.table)
            for operator in action.operators or []:
                if not isinstance(operator, Summary):
                    continue
                trend = operator.trend
                if trend is None:
                    continue
                summary_table = self.table_name_for_summary(
                    self._collection,
                    operator.table if operator.table is not None else table,
                    position,
                    operator.table is not None,
                    collection,
                    table,
                )
                trend_table = self.table_name_for_trends(
                    self._collection,
                    trend.table if trend.table is not None else table,
                    position,
                    trend.table is not None,
                    collection,
                    table,
                )
                dq_trend_fn, project_root = build_trend_tfr_mod(
                    days=trend.days, weeks=trend.weeks, months=trend.months
                )
                transformers.append(
                    TrendTransformerInfo(
                        info=TransformerInfo(
                            func=dq_trend_fn,
                            input_tables=[
                                f"{summary_table.collection}/"
                                f"{summary_table.table}@HEAD",
                                f"{trend_table.table}@HEAD",
                            ],
                            output_tables=[trend_table.table],
                            trigger_by=None,
                            on_tables=None,
                        ),
                        project_root=project_root,
                    )
                )
        return transformers

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

        raise DataQualityErrorCode.DQPE_1.exception(table=action_table)

    def resolve_output_table(self, action_table: str) -> tuple[str, str, int]:
        if action_table in self.output_tables:
            return (
                self.collection,
                action_table,
                self.output_tables[action_table].position,
            )

        raise DataQualityErrorCode.DQPE_2.exception(table=action_table)

    def build_table_names(
        self,
        collection: str,
        table: str,
        position: int | None,
        operations: list[Operator],
    ) -> list[TableInfo]:
        table_names: list[TableInfo] = []
        if operations:
            for operation in operations:
                match operation:
                    case Fail():
                        pass
                    case Enrich():
                        if operation.to_table is not None:
                            table_name = operation.to_table
                            table_names.append(
                                self.table_name_for_enrich(
                                    self._collection,
                                    table_name,
                                    position,
                                    collection,
                                    table,
                                )
                            )
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
                        has_custom_name = operation.table is not None
                        table_name = operation.table if has_custom_name else table
                        table_names.append(
                            self.table_name_for_summary(
                                self._collection,
                                table_name,
                                position,
                                has_custom_name,
                                collection,
                                table,
                            )
                        )
                    case _:
                        raise DataQualityErrorCode.DQPE_3.exception(
                            operation=type(operation)
                        )
        return table_names

    @classmethod
    def table_name_for_enrich(
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
            kind=TableKind.DQ_ENRICH,
            is_custom=True,
            from_table=from_table,
            from_collection=from_collection,
        )

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
    def table_name_for_trends(
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
            kind=TableKind.DQ_SUMMARY_TRENDS,
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
            raise DataQualityErrorCode.DQPE_4.exception(
                report="\n".join(duplicates_report)
            )

    @classmethod
    def validate_no_multiple_times_overridden_table_names(
        cls,
        overridden_input_tables: Dict[str, int],
        overridden_output_tables: Dict[str, int],
    ) -> None:
        overridden_tables_with_errors: list[str] = []
        for table, count in overridden_input_tables.items():
            if count > 1:
                overridden_tables_with_errors.append(
                    f"- Input table '{table}': {count}."
                )
        for table, count in overridden_output_tables.items():
            if count > 1:
                overridden_tables_with_errors.append(
                    f"- Output table '{table}': {count}."
                )
        if overridden_tables_with_errors:
            report = "\n".join(overridden_tables_with_errors)
            raise DataQualityErrorCode.DQPE_15.exception(report=report)

    def tables_data(
        self,
        input_table_frames: InputTableSpec,
        output_table_frames: OutputTableSpec,
        derived_table_frames: DerivedTableFrameSpec,
    ):
        input_table_frames_vault = list(input_table_frames)
        output_table_frames_vault = list(output_table_frames)

        self.run_actions(
            self.input_actions,
            self.resolve_input_table,
            input_table_frames,
            input_table_frames_vault,
            DataQualityErrorCode.DQPE_5,
            derived_table_frames,
        )

        self.run_actions(
            self.output_actions,
            self.resolve_output_table,
            output_table_frames,
            output_table_frames_vault,
            DataQualityErrorCode.DQPE_6,
            derived_table_frames,
        )

        input_table_frames.clear()
        input_table_frames.extend(input_table_frames_vault)
        output_table_frames.clear()
        output_table_frames.extend(output_table_frames_vault)

    def run_actions(
        self,
        actions: list[DataQuality],
        resolve_table: Callable[[str], Tuple[str, str, int]],
        table_frames: InputTableSpec | OutputTableSpec,
        table_frames_vault: list,
        position_error_code: DataQualityErrorCode,
        derived_table_frames: DerivedTableFrameSpec,
    ):
        for action in actions:
            collection, table, position = resolve_table(action.table)
            table_infos = self.build_table_names(
                collection, table, position, action.operators
            )
            if position >= len(table_frames):
                raise position_error_code.exception(
                    position=position,
                    table=action.table,
                    count=len(table_frames),
                )
            tableframe = table_frames[position]
            derived_table_frame, schema, dq_cols, dq_tags = self.compute(
                tableframe, action
            )

            if derived_table_frame is None:
                continue

            table_producing_operators = sum(
                1 for operator in action.operators if self.producing(operator)
            )

            table_info_idx = 0
            for operation in action.operators:
                if self.producing(operation):
                    if table_info_idx >= table_producing_operators:
                        raise DataQualityErrorCode.DQPE_8.exception(
                            table=action.table,
                            index=table_info_idx,
                            producing=table_producing_operators,
                            infos=len(table_infos),
                            operators=len(action.operators),
                        )
                    table_info = table_infos[table_info_idx]
                else:
                    table_info = None
                harvest = self.harvest(
                    derived_table_frame,
                    schema,
                    dq_cols,
                    dq_tags,
                    table_info,
                    operation,
                )
                if harvest.output is not None:
                    table_frames_vault[position] = harvest.output.derived_tf
                if harvest.derived is not None:
                    derived_table_frames.append(harvest.derived)
                    table_info_idx += 1
            if table_info_idx != table_producing_operators:
                raise DataQualityErrorCode.DQPE_8.exception(
                    table=action.table,
                    index=table_info_idx,
                    producing=table_producing_operators,
                    infos=len(table_infos),
                    operators=len(action.operators),
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
        td_typing.Schema | None,
        dict[str, ColumnInfo] | None,
        dict[str, set[str]] | None,
    ]:
        tf = cls.to_tableframe(tableframe)
        if tf is None:
            return None, None, None, None

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
            dq = dq._classifier_plan(classifier=classifier)

        dtf.derived_tf = dq.tf()
        dtf.status = DerivedTableFrameStatus.COMPUTED
        return dtf, dq._schema, dq._cols, dq._tags

    @classmethod
    def harvest(
        cls,
        dtf_i: DerivedTableFrame | None,
        schema: td_typing.Schema,
        cols: dict[str, ColumnInfo],
        tags: dict[str, set[str]] | None,
        _table_info: TableInfo | None,
        operation: Operator,
    ) -> Harvest:
        if dtf_i is None:
            return Harvest(output=None, derived=None)
        tf_i = dtf_i.derived_tf
        if tf_i is None:
            return Harvest(output=None, derived=None)

        dq = DataQualityEngine._new(
            tf=dtf_i.derived_tf,
            dq_i=None,
            schema=schema,
            cols=cols,
            tags=tags,
        )

        match operation:
            case Fail():
                return dq.fail(operation)
            case Enrich():
                return dq.enrich(operation)
            case Filter():
                return dq.filter(operation)
            case Select():
                return dq.select(operation)
            case Summary():
                return dq.summary(operation)
            case _:
                raise DataQualityErrorCode.DQPE_3.exception(operation=type(operation))

    @classmethod
    def producing(cls, operator: Operator) -> bool:
        if operator is None:
            return False
        return (
            not isinstance(operator, Fail)
            and not (isinstance(operator, Enrich) and operator.to_table is None)
            and not (isinstance(operator, Filter) and operator.to_table is None)
        )

    @classmethod
    def overriding(cls, operator: Operator) -> bool:
        if operator is None:
            return False
        return (isinstance(operator, Enrich) and operator.to_table is None) or (
            isinstance(operator, Filter)
        )
