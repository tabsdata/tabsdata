#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from typing import Self

from tabsdatak.tableframe._core.base import BaseFrame
from tabsdatak.tableframe._engine.model.tableframe import TableFrameState


class EngineFrame(BaseFrame):
    _ACCESS_KEY = "tdgrant"
    _ACCESS_SECRET = object()

    @classmethod
    def _check_access(cls, kwargs) -> None:
        if kwargs.pop(cls._ACCESS_KEY, None) is not cls._ACCESS_SECRET:
            from tabsdatak.tableframe._engine.event.error import TableFrameErrorCode

            raise TableFrameErrorCode.TF1.exception()

    def __new__(cls, *args, **kwargs) -> Self:
        cls._check_access(kwargs)
        return object.__new__(cls)

    def __init__(self, data=None, schema=None, **kwargs) -> None:
        self._check_access(kwargs)
        super().__init__(data, schema, **kwargs)
        self._state = TableFrameState.empty()
        from tabsdatak.tableframe._core.translate import validate_schema

        validate_schema(self._lf)

    @classmethod
    def _wrap(cls, lf, state=None) -> Self:
        obj = cls.__new__(cls, **{cls._ACCESS_KEY: cls._ACCESS_SECRET})
        obj._lf = lf
        obj._state = state if state is not None else TableFrameState.empty()
        return obj

    def _clone(self, lf) -> Self:
        state = self._state.clone() if self._state is not None else None
        return type(self)._wrap(lf, state)
