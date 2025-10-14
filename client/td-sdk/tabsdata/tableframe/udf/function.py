#
#  Copyright 2025 Tabs Data Inc.
#

from abc import ABC
from typing import Any, Union

import tabsdata.tableframe.typing as td_typing


class UDF(ABC):
    def __init__(self):
        if self.__class__ is UDF:
            raise TypeError(
                "Cannot instantiate UDF directly. Create a subclass instead."
            )

        self._on_batch_is_overridden = self.__class__.on_batch is not UDF.on_batch
        self._on_element_is_overridden = self.__class__.on_element is not UDF.on_element

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        call_is_overridden = cls.__call__ is not UDF.__call__
        if call_is_overridden:
            raise TypeError(f"{cls.__name__} must not override '__call__' method")

        on_batch_is_overridden = cls.on_batch is not UDF.on_batch
        on_element_is_overridden = cls.on_element is not UDF.on_element

        if not (on_batch_is_overridden or on_element_is_overridden):
            raise TypeError(
                f"{cls.__name__} must override at least one of"
                f" '{UDF.on_element.__name__}' or '{UDF.on_batch.__name__}' methods"
            )

        if on_batch_is_overridden and on_element_is_overridden:
            raise TypeError(
                f"{cls.__name__} must override only one of"
                f" '{UDF.on_element.__name__}' and '{UDF.on_batch.__name__}' methods"
            )

    def on_batch(self, series: list[td_typing.Series]) -> list[td_typing.Series]:
        pass

    def on_element(self, values: list[Any]) -> list[Any]:
        pass

    def __call__(self, data: list[td_typing.Series]) -> list[td_typing.Series]:
        if self._on_batch_is_overridden:
            return self.on_batch(data)
        elif self._on_element_is_overridden:
            if not data:
                return []
            h_series = []
            for values in zip(*data):
                h_values = self.on_element(list(values))
                h_series.append(h_values)
            v_series = zip(*h_series)
            return [td_typing.Series(values=list(column)) for column in v_series]
        else:
            raise RuntimeError(
                f"{self.__class__.__name__} has neither on_batch nor on_element"
                " overridden"
            )
