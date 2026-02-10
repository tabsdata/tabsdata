#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Union

from tabsdata._utils.tableframe._constants import (
    EMPTY_EXECUTION,
    EMPTY_TIMESTAMP,
    EMPTY_TRANSACTION,
    EMPTY_VERSION,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass(frozen=True, slots=True)
class TableFrameProperties:
    transaction: str
    execution: str
    version: str
    timestamp: datetime

    def __str__(self):
        return (
            "TableFrameProperties("
            f"transaction={self.transaction!r}, "
            f"execution={self.execution!r}, "
            f"version={self.version!r}, "
            f"timestamp={self.timestamp!r}"
            ")"
        )

    def __repr__(self):
        return self.__str__()

    @property
    def execution(self) -> str:
        return self.execution

    @property
    def transaction(self) -> str:
        return self.transaction

    @property
    def version(self) -> str:
        return self.version

    @property
    def timestamp(self) -> datetime:
        return self.timestamp

    @classmethod
    def builder(cls) -> "TableFramePropertiesBuilder":
        return TableFramePropertiesBuilder()


class TableFramePropertiesBuilder:
    def __init__(self):
        self._execution: Optional[str] = None
        self._transaction: Optional[str] = None
        self._version: Optional[str] = None
        self._timestamp: Optional[datetime] = None

    def with_execution(self, execution: str) -> "TableFramePropertiesBuilder":
        self._execution = execution
        return self

    def with_transaction(self, transaction: str) -> "TableFramePropertiesBuilder":
        self._transaction = transaction
        return self

    def with_version(self, version: str) -> "TableFramePropertiesBuilder":
        self._version = version
        return self

    def with_timestamp(
        self, timestamp: Union[str, int, datetime]
    ) -> "TableFramePropertiesBuilder":
        if isinstance(timestamp, datetime):
            self._timestamp = timestamp
        elif isinstance(timestamp, (str, int)):
            try:
                timestamp = (
                    int(timestamp.strip()) if isinstance(timestamp, str) else timestamp
                )
                self._timestamp = datetime.fromtimestamp(
                    timestamp / 1000, tz=timezone.utc
                )
            except (ValueError, OSError) as error:
                raise ValueError(
                    f"Invalid timestamp value: {timestamp!r}. Expected a valid "
                    "integer or "
                    "string or "
                    "datetime "
                    "in milliseconds or a datetime object."
                ) from error
        else:
            raise TypeError(
                "Invalid timestamp; must be str, int, or datetime, "
                f"got {type(timestamp).__name__}"
            )
        return self

    @staticmethod
    def empty() -> TableFrameProperties:
        return (
            TableFrameProperties.builder()
            .with_execution(EMPTY_EXECUTION)
            .with_transaction(EMPTY_TRANSACTION)
            .with_version(EMPTY_VERSION)
            .with_timestamp(EMPTY_TIMESTAMP)
            .build()
        )

    def build(self) -> TableFrameProperties:
        return TableFrameProperties(
            execution=self._execution,
            transaction=self._transaction,
            version=self._version,
            timestamp=self._timestamp,
        )
