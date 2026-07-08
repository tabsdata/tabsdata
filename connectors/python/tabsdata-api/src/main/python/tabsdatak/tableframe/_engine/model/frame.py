#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from tabsdatak.tableframe._engine.constant.emptiness import (
    EMPTY_EXECUTION,
    EMPTY_TIMESTAMP,
    EMPTY_TRANSACTION,
    EMPTY_VERSION,
)
from tabsdatak.tableframe._engine.taxonomy.frame import TableFrameOrigin

if TYPE_CHECKING:
    from tabsdatak.extensions.tableframe._engine.taxonomy.aspect import (
        Aspect as Aspect_,
    )


@dataclass(frozen=True, slots=True)
class DataVersion:
    transaction: str
    execution: str
    version: str
    timestamp: datetime

    @classmethod
    def builder(cls) -> DataVersionBuilder:
        return DataVersionBuilder()

    @classmethod
    def empty(cls) -> DataVersion:
        return DataVersionBuilder.empty()

    def clone(self) -> DataVersion:
        # No need to clone as its dataclass is frozen and immutable.
        return self


class DataVersionBuilder:
    def __init__(self) -> None:
        self.execution: str | None = None
        self.transaction: str | None = None
        self.version: str | None = None
        self.timestamp: datetime | None = None

    def with_execution(self, execution: str) -> DataVersionBuilder:
        self.execution = execution
        return self

    def with_transaction(self, transaction: str) -> DataVersionBuilder:
        self.transaction = transaction
        return self

    def with_version(self, version: str) -> DataVersionBuilder:
        self.version = version
        return self

    def with_timestamp(self, timestamp: str | int | datetime) -> DataVersionBuilder:
        if isinstance(timestamp, datetime):
            self.timestamp = timestamp
            return self
        if isinstance(timestamp, (str, int)):
            try:
                milliseconds = (
                    int(timestamp.strip()) if isinstance(timestamp, str) else timestamp
                )
                self.timestamp = datetime.fromtimestamp(
                    milliseconds / 1000, tz=timezone.utc
                )
            except (ValueError, OSError) as error:
                raise ValueError(
                    f"Invalid timestamp value: {timestamp!r}. Expected a valid "
                    "integer, string, or datetime in milliseconds."
                ) from error
            return self
        raise TypeError(
            "Invalid timestamp; must be str, int, or datetime, "
            f"got {type(timestamp).__name__}"
        )

    @staticmethod
    def empty() -> DataVersion:
        return (
            DataVersion
            .builder()
            .with_execution(EMPTY_EXECUTION)
            .with_transaction(EMPTY_TRANSACTION)
            .with_version(EMPTY_VERSION)
            .with_timestamp(EMPTY_TIMESTAMP)
            .build()
        )

    def build(self) -> DataVersion:
        execution = self.execution
        transaction = self.transaction
        version = self.version
        timestamp = self.timestamp
        if (
            execution is None
            or transaction is None
            or version is None
            or timestamp is None
        ):
            from tabsdatak.tableframe._engine.event.error import TableFrameErrorCode

            raise TableFrameErrorCode.TF2.exception(
                execution=execution,
                transaction=transaction,
                version=version,
                timestamp=timestamp,
            )
        return DataVersion(
            execution=execution,
            transaction=transaction,
            version=version,
            timestamp=timestamp,
        )


@dataclass(frozen=True, slots=True)
class DataGenesis:
    id: str | None = None
    idx: int | None = None
    origin: TableFrameOrigin | None = None

    def clone(self) -> DataGenesis:
        # No need to clone as its dataclass is frozen and immutable.
        return self


@dataclass(slots=True)
class TableFrameState:
    version: DataVersion | None = None
    genesis: DataGenesis | None = None
    aspects: dict[Aspect_, object] = field(default_factory=dict)

    @classmethod
    def empty(cls) -> TableFrameState:
        return cls(version=None, genesis=None, aspects={})

    def clone(self) -> TableFrameState:
        return TableFrameState(
            version=None if self.version is None else self.version.clone(),
            genesis=None if self.genesis is None else self.genesis.clone(),
            aspects=dict(self.aspects),
        )
