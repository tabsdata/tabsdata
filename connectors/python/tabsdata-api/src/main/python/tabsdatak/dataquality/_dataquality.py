#
# Copyright 2026 Tabsdata Inc.
#

"""
This module defines the `DataQuality` action, which orchestrates the application
of data quality rules on a table. It combines classifiers and operators to perform
comprehensive data quality checks and actions.
"""

from typing import Annotated, TypeAlias

from pydantic import model_validator

# noinspection PyProtectedMember
from tabsdatak import _api
from tabsdatak.api import OnTablesAction, TableNameSpec
from tabsdatak.dataquality._classifier import Classifier
from tabsdatak.dataquality._error import DataQualityErrorCode
from tabsdatak.dataquality._operator import Operator
from tabsdatak.tableframe._engine.constant.column.prefixes import (
    TD_CONNECTOR_SYSTEM_COLUMN_PREFIX,
    TD_SYSTEM_COLUMN_PREFIX,
)

RESERVED_COLUMN_PREFIXES = (
    TD_SYSTEM_COLUMN_PREFIX,
    TD_CONNECTOR_SYSTEM_COLUMN_PREFIX,
)


def reserved_prefix(name: str) -> str | None:
    for prefix in RESERVED_COLUMN_PREFIXES:
        if name.startswith(prefix):
            return prefix
    return None


def declared_columns(classifier: Classifier) -> list[tuple[str, str | None]]:
    spec = classifier.column_names
    if spec is None:
        return []
    if isinstance(spec, str):
        return [(spec, None)]
    if isinstance(spec, tuple):
        return [(spec[0], spec[1])]
    pairs = []
    for entry in spec:
        if isinstance(entry, str):
            pairs.append((entry, None))
        elif isinstance(entry, tuple):
            pairs.append((entry[0], entry[1]))
    return pairs


ClassifiersSpec: TypeAlias = Annotated[list[Classifier], _api.NON_EMPTY_LIST_FIELD]
"""A non-empty list of `Classifier` instances."""

OperatorsSpec: TypeAlias = Annotated[list[Operator], _api.NON_EMPTY_LIST_FIELD]
"""A non-empty list of `Operator` instances."""


@_api.dataclass(DataQualityErrorCode.DQC_201)
class DataQuality(OnTablesAction):
    """A data quality check applied to one of a function's output tables.

    :categories: dq-action

    Bundles classifiers (which label each row) with operators (which act on
    those labels). Pass it to a `@publisher` / `@transformer` /
    `@subscriber` via `on_tables=[DataQuality(...)]`; the runtime runs
    the check against the named output table after the function succeeds.

    Args:
        table: Output table the check runs on; a valid table name
            (alphanumerics and underscores, starting with a letter).
        classifiers: Classifiers that label the table's columns; a
            non-empty list (at least one).
        operators: Operators applied to the labels -- `Enrich`,
            `Summary`, `Filter`, `Select` or `Fail`; a non-empty
            list (at least one).

    Examples:
        .. code-block:: python

            DataQuality(
                table="checked",
                classifiers=[IsNull(["value"])],
                operators=[Enrich(), Summary()],
            )
    """

    # devguide-ref: how classifiers/operators compose into a DataQuality check

    table: TableNameSpec
    classifiers: ClassifiersSpec
    operators: OperatorsSpec

    @model_validator(mode="after")
    def validate_verdict_columns(self) -> "DataQuality":
        claims: dict[tuple[type, str] | str, list[tuple[str, str, str]]] = {}
        aliases: dict[str, tuple[str, str]] = {}
        sources: dict[str, str] = {}
        for classifier in self.classifiers:
            name = type(classifier).__name__
            for source, verdict in declared_columns(classifier):
                sources.setdefault(source, name)
                if verdict is None:
                    key = (type(classifier), source)
                    column = f"inferred from source column '{source}'"
                else:
                    prefix = reserved_prefix(verdict)
                    if prefix is not None:
                        raise DataQualityErrorCode.DQC_203.exception(
                            column=verdict,
                            classifier=name,
                            prefix=prefix,
                        )
                    if verdict == source:
                        raise DataQualityErrorCode.DQC_204.exception(
                            classifier=name,
                            column=verdict,
                        )
                    key = verdict
                    column = f"'{verdict}'"
                    aliases.setdefault(verdict, (name, source))
                claims.setdefault(key, []).append((name, source, column))

        for verdict, (producer, source) in aliases.items():
            consumer = sources.get(verdict)
            if consumer is not None:
                raise DataQualityErrorCode.DQC_205.exception(
                    classifier=consumer,
                    column=verdict,
                    producer=producer,
                    source=source,
                )

        for colliding in claims.values():
            if len(colliding) < 2:
                continue
            name, source, column = colliding[0]
            detail = ", ".join(
                f"{claim_name} on '{claim_source}'"
                for claim_name, claim_source, _ in colliding
            )
            raise DataQualityErrorCode.DQC_202.exception(
                column=column,
                count=len(colliding),
                detail=detail,
                classifier=name,
                source=source,
            )
        return self
