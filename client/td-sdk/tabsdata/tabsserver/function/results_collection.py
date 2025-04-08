#
# Copyright 2025 Tabs Data Inc.
#
from __future__ import annotations

import logging
from typing import List, Tuple, Union

import tabsdata as td
from tabsdata.utils.tableframe._reflection import check_required_columns

logger = logging.getLogger(__name__)

VALID_SINGLE_RESULT = Union[td.TableFrame | None | List[td.TableFrame | None]]


class ResultsCollection:

    def __init__(self, results: Tuple[VALID_SINGLE_RESULT] | VALID_SINGLE_RESULT):
        if isinstance(results, tuple):
            self.results = results
        else:
            self.results = (results,)
        self.results = tuple(Result(result) for result in self.results)

    def __len__(self):
        return len(self.results)

    def __getitem__(self, index: int) -> Result:
        return self.results[index]

    def __repr__(self):
        return f"ResultsCollection({self.results.__repr__()})"

    def __str__(self):
        return f"ResultsCollection({self.results.__str__()})"

    def __iter__(self) -> Result:
        for result in self.results:
            yield result

    def check_collection_integrity(self):
        for result in self.results:
            result.check_integrity()

    def convert_none_to_empty_frame(self):
        self.results = tuple(
            result.convert_none_to_empty_frame() for result in self.results
        )


class Result:
    def __init__(self, result: VALID_SINGLE_RESULT):
        self.value = result

    def __repr__(self):
        return f"Result({self.value.__repr__()})"

    def __str__(self):
        return f"Result({self.value.__str__()})"

    def check_integrity(self):
        if self.value is None:
            pass
        elif isinstance(self.value, td.TableFrame):
            # noinspection PyProtectedMember
            check_required_columns(self.value._to_lazy())
        elif isinstance(self.value, list):
            for table in self.value:
                if table is None:
                    pass
                elif isinstance(table, td.TableFrame):
                    # noinspection PyProtectedMember
                    check_required_columns(table._to_lazy())
                else:
                    raise TypeError(f"Invalid result type in list '{type(table)}'")
        else:
            raise TypeError(f"Invalid result type '{type(self.value)}'")

    def convert_none_to_empty_frame(self):
        self.value = _convert_none_to_empty_frame(self.value)
        return self


def _convert_none_to_empty_frame(
    results: VALID_SINGLE_RESULT,
) -> td.TableFrame | List[td.TableFrame]:
    if results is None:
        logger.debug("Result is None. Returning empty frame.")
        return td.TableFrame({})
    elif isinstance(results, td.TableFrame):
        return results
    elif isinstance(results, list):
        return [_convert_none_to_empty_frame(table) for table in results]
    else:
        raise TypeError(f"Invalid result type: {type(results)}")
