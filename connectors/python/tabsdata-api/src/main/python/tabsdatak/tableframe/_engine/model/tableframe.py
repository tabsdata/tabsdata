#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from tabsdatak.tableframe._engine.constant.emptiness import (
    EMPTY_EXECUTION,
    EMPTY_TIMESTAMP,
    EMPTY_TRANSACTION,
    EMPTY_VERSION,
)


@dataclass(frozen=True, slots=True)
class DataVersionProperties:
    transaction: str
    execution: str
    version: str
    timestamp: datetime

    @classmethod
    def builder(cls) -> DataVersionPropertiesBuilder:
        return DataVersionPropertiesBuilder()


class DataVersionPropertiesBuilder:
    def __init__(self) -> None:
        self.execution: str | None = None
        self.transaction: str | None = None
        self.version: str | None = None
        self.timestamp: datetime | None = None

    def with_execution(self, execution: str) -> DataVersionPropertiesBuilder:
        self.execution = execution
        return self

    def with_transaction(self, transaction: str) -> DataVersionPropertiesBuilder:
        self.transaction = transaction
        return self

    def with_version(self, version: str) -> DataVersionPropertiesBuilder:
        self.version = version
        return self

    def with_timestamp(
        self, timestamp: str | int | datetime
    ) -> DataVersionPropertiesBuilder:
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
    def empty() -> DataVersionProperties:
        return (
            DataVersionProperties.builder()
            .with_execution(EMPTY_EXECUTION)
            .with_transaction(EMPTY_TRANSACTION)
            .with_version(EMPTY_VERSION)
            .with_timestamp(EMPTY_TIMESTAMP)
            .build()
        )

    def build(self) -> DataVersionProperties:
        return DataVersionProperties(
            execution=self.execution,
            transaction=self.transaction,
            version=self.version,
            timestamp=self.timestamp,
        )


@dataclass(slots=True)
class TableFrameState:
    properties: DataVersionProperties | None = None

    @classmethod
    def empty(cls) -> TableFrameState:
        return cls(properties=None)

    def clone(self) -> TableFrameState:
        return TableFrameState(properties=self.properties)
