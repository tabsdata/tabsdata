#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import Any

import polars as pl

from tabsdatak.server._tdrunner.function._commons.context.request import (
    ContextValueError,
    FromFlowRequestCtx,
)

USER_NAMESPACE_COLUMN = "user_namespace"


class InvalidFunctionStateError(ValueError):
    pass


class FunctionState:
    values: dict[str, Any]

    def __init__(self, values: dict[str, Any]) -> None:
        self.values = values

    @classmethod
    def of(cls, frame: pl.DataFrame, name: str) -> FunctionState:
        if frame.width == 0:
            return cls({})
        if frame.columns != [USER_NAMESPACE_COLUMN]:
            raise InvalidFunctionStateError(
                f"State {name} must have the single column "
                f"{USER_NAMESPACE_COLUMN!r}; got {frame.columns!r}"
            )
        if frame.height != 1:
            raise InvalidFunctionStateError(
                f"State {name} has {frame.height} row(s); expected 1"
            )
        return cls(dict(frame.item(0, USER_NAMESPACE_COLUMN) or {}))

    def validate(self) -> None:
        for key, value in self.values.items():
            try:
                FromFlowRequestCtx.validate_key(key)
                FromFlowRequestCtx.validate_value(key, value)
            except ContextValueError as error:
                raise InvalidFunctionStateError(str(error)) from error.__cause__

    def frame(self) -> pl.DataFrame:
        if self.values:
            return pl.DataFrame({USER_NAMESPACE_COLUMN: [self.values]})
        return pl.DataFrame()
