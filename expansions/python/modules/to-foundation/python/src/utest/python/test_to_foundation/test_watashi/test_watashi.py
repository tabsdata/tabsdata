#
# Copyright 2025 Tabsdata Inc.
#

import pytest

from tabsdata.expansions.foundation.watashi import (
    CONSTANT_F64,
    CONSTANT_I32,
    CONSTANT_STR,
    DataObject,
    EnumAuto,
    EnumString,
    _py,
    string_function,
)


@pytest.mark.pyo3
def test_constant_str():
    assert CONSTANT_STR == "komi"


@pytest.mark.pyo3
def test_constant_i32():
    assert CONSTANT_I32 == 19


@pytest.mark.pyo3
def test_constant_f64():
    assert CONSTANT_F64 == 6.5


@pytest.mark.pyo3
def test_enum_auto():
    fuseki = EnumAuto.Fuseki
    chuban = EnumAuto.Chuban
    yose = EnumAuto.Yose
    assert fuseki != chuban
    assert chuban != yose
    assert fuseki == EnumAuto.Fuseki
    assert chuban == EnumAuto.Chuban
    assert yose == EnumAuto.Yose


@pytest.mark.pyo3
def test_enum_string():
    fuseki = EnumString.Fuseki
    chuban = EnumString.Chuban
    yose = EnumString.Yose
    assert fuseki != chuban
    assert chuban != yose
    assert fuseki == EnumString.Fuseki
    assert chuban == EnumString.Chuban
    assert yose == EnumString.Yose
    assert fuseki.value == "f"
    assert chuban.value == "c"
    assert yose.value == "y"


@pytest.mark.pyo3
def test_data_object():
    data = DataObject("Takemiya", "Kobayashi", 5, EnumString.Yose)
    assert data.kuro == "Takemiya"
    assert data.shiro == "Kobayashi"
    assert data.me == 5
    assert data.sen == EnumString.Yose

    info = data.get_info()
    assert info == "DataObject(kuro=Takemiya, shiro=Kobayashi, me=5, sen=Yose)"

    data.point()
    assert data.me == 6
    data.point()
    assert data.me == 7


@pytest.mark.pyo3
def test_py_function():
    result = _py()
    assert result is None


@pytest.mark.pyo3
def test_string_function():
    result = string_function("WA")
    assert result == "wa"
