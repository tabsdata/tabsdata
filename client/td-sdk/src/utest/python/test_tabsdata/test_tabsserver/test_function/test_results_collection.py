#
# Copyright 2025 Tabs Data Inc.
#

import pytest

import tabsdata as td
from tabsdata._tabsserver.function.results_collection import Result, ResultsCollection
from tests_tabsdata.conftest import clean_polars_df

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

CORRECT_TF = td.TableFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


def test_result_class():
    result = Result(None)
    assert result.value is None
    result = Result([None, None])
    assert result.value == [None, None]
    result = Result(CORRECT_TF)
    assert result.value == CORRECT_TF
    result = Result([CORRECT_TF, None, CORRECT_TF])
    assert result.value == [CORRECT_TF, None, CORRECT_TF]


def test_result_class_check_integrity():
    result = Result(None)
    result.check_integrity()

    result = Result([None, None])
    result.check_integrity()

    result = Result(CORRECT_TF)
    result.check_integrity()

    result = Result([CORRECT_TF, None, CORRECT_TF])
    result.check_integrity()

    result = Result([CORRECT_TF, 3, CORRECT_TF])
    with pytest.raises(TypeError):
        result.check_integrity()

    result = Result(3)
    with pytest.raises(TypeError):
        result.check_integrity()


def test_results_collection_class():
    results = ResultsCollection(None)
    assert results.results[0].value is None

    results = ResultsCollection(CORRECT_TF)
    assert isinstance(results.results[0].value, td.TableFrame)

    results = ResultsCollection([CORRECT_TF, None, CORRECT_TF])
    assert isinstance(results.results[0].value[0], td.TableFrame)
    assert results.results[0].value[1] is None
    assert isinstance(results.results[0].value[2], td.TableFrame)

    results = ResultsCollection([CORRECT_TF, 3, CORRECT_TF])
    assert isinstance(results.results[0].value[0], td.TableFrame)
    assert results.results[0].value[1] == 3
    assert isinstance(results.results[0].value[2], td.TableFrame)

    results = ResultsCollection(3)
    assert results.results[0].value == 3

    results = ResultsCollection((CORRECT_TF, 3, CORRECT_TF))
    assert isinstance(results.results[0].value, td.TableFrame)
    assert results.results[1].value == 3
    assert isinstance(results.results[2].value, td.TableFrame)


def test_results_collection_len():
    results = ResultsCollection(None)
    assert len(results) == 1

    results = ResultsCollection(CORRECT_TF)
    assert len(results) == 1

    results = ResultsCollection([CORRECT_TF, None, CORRECT_TF])
    assert len(results) == 1

    results = ResultsCollection([CORRECT_TF, 3, CORRECT_TF])
    assert len(results) == 1

    results = ResultsCollection(3)
    assert len(results) == 1

    results = ResultsCollection((CORRECT_TF, 3, CORRECT_TF))
    assert len(results) == 3


def test_results_collection_get_item():
    results = ResultsCollection(None)
    assert results[0].value is None

    results = ResultsCollection(CORRECT_TF)
    assert results[0].value == CORRECT_TF

    results = ResultsCollection([CORRECT_TF, None, CORRECT_TF])
    assert results[0].value == [CORRECT_TF, None, CORRECT_TF]

    results = ResultsCollection([CORRECT_TF, 3, CORRECT_TF])
    assert results[0].value == [CORRECT_TF, 3, CORRECT_TF]

    results = ResultsCollection(3)
    assert results[0].value == 3

    results = ResultsCollection((CORRECT_TF, 3, CORRECT_TF))
    assert results[0].value == CORRECT_TF
    assert results[1].value == 3
    assert results[2].value == CORRECT_TF


def test_results_collection_iter():
    results = ResultsCollection(None)
    for result in results:
        assert result.value is None

    results = ResultsCollection(CORRECT_TF)
    for result in results:
        assert result.value == CORRECT_TF

    results = ResultsCollection([CORRECT_TF, None, CORRECT_TF])
    for result in results:
        assert result.value == [CORRECT_TF, None, CORRECT_TF]

    results = ResultsCollection([CORRECT_TF, 3, CORRECT_TF])
    for result in results:
        assert result.value == [CORRECT_TF, 3, CORRECT_TF]

    results = ResultsCollection(3)
    for result in results:
        assert result.value == 3

    results = ResultsCollection((CORRECT_TF, 3, CORRECT_TF))
    index = 0
    for result in results:
        if index == 0:
            assert result.value == CORRECT_TF
        if index == 1:
            assert result.value == 3
        if index == 2:
            assert result.value == CORRECT_TF
        if index > 2:
            raise AssertionError
        index += 1


def test_results_collection_check_collection_integrity():
    results = ResultsCollection(None)
    results.check_collection_integrity()

    results = ResultsCollection(CORRECT_TF)
    results.check_collection_integrity()

    results = ResultsCollection([CORRECT_TF, None, CORRECT_TF])
    results.check_collection_integrity()

    results = ResultsCollection([CORRECT_TF, 3, CORRECT_TF])
    with pytest.raises(TypeError):
        results.check_collection_integrity()

    results = ResultsCollection(3)
    with pytest.raises(TypeError):
        results.check_collection_integrity()

    results = ResultsCollection((CORRECT_TF, 3, CORRECT_TF))
    with pytest.raises(TypeError):
        results.check_collection_integrity()

    results = ResultsCollection((CORRECT_TF, None, CORRECT_TF))
    results.check_collection_integrity()


def test_convert_none_to_empty_frame():
    results = ResultsCollection(None)
    results.convert_none_to_empty_frame()
    assert isinstance(results.results[0].value, td.TableFrame)
    df = results.results[0].value._to_lazy().collect()
    assert clean_polars_df(df).is_empty()

    results = ResultsCollection(CORRECT_TF)
    results.convert_none_to_empty_frame()
    assert results.results[0].value == CORRECT_TF

    results = ResultsCollection([CORRECT_TF, None, CORRECT_TF])
    results.convert_none_to_empty_frame()
    assert results.results[0].value[0] == CORRECT_TF
    assert isinstance(results.results[0].value[1], td.TableFrame)
    df = results.results[0].value[1]._to_lazy().collect()
    assert clean_polars_df(df).is_empty()
    assert results.results[0].value[2] == CORRECT_TF

    results = ResultsCollection((CORRECT_TF, None, CORRECT_TF))
    results.convert_none_to_empty_frame()
    assert results.results[0].value == CORRECT_TF
    assert isinstance(results.results[1].value, td.TableFrame)
    df = results.results[1].value._to_lazy().collect()
    assert clean_polars_df(df).is_empty()
    assert results.results[2].value == CORRECT_TF
