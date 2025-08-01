#
# Copyright 2024 Tabs Data Inc.
#

import pytest

from tabsdata.exceptions import (
    DecoratorConfigurationError,
    DestinationConfigurationError,
    ErrorCode,
    FunctionConfigurationError,
    RegistrationError,
    SDKError,
    SourceConfigurationError,
)


def test_function_configuration_error():
    with pytest.raises(FunctionConfigurationError) as e:
        raise FunctionConfigurationError(ErrorCode.FCE1, "dummy")
    assert e.value.code == ErrorCode.FCE1.value.get("code")
    assert e.value.error_code == ErrorCode.FCE1


def test_function_configuration_error_wrong_code_fails():
    with pytest.raises(SDKError) as e:
        raise FunctionConfigurationError(ErrorCode.RE1)
    assert e.value.code == ErrorCode.SDKE1.value.get("code")
    assert e.value.error_code == ErrorCode.SDKE1


def test_input_configuration_error():
    with pytest.raises(SourceConfigurationError) as e:
        raise SourceConfigurationError(ErrorCode.SOCE1, "dummy", "dummy", "dummy")
    assert e.value.code == ErrorCode.SOCE1.value.get("code")
    assert e.value.error_code == ErrorCode.SOCE1


def test_input_configuration_error_wrong_code_fails():
    with pytest.raises(SDKError) as e:
        raise SourceConfigurationError(ErrorCode.RE1)
    assert e.value.code == ErrorCode.SDKE1.value.get("code")
    assert e.value.error_code == ErrorCode.SDKE1


def test_output_configuration_error():
    with pytest.raises(DestinationConfigurationError) as e:
        raise DestinationConfigurationError(ErrorCode.DECE1, "dummy", "dummy", "dummy")
    assert e.value.code == ErrorCode.DECE1.value.get("code")
    assert e.value.error_code == ErrorCode.DECE1


def test_output_configuration_error_wrong_code_fails():
    with pytest.raises(SDKError) as e:
        raise DestinationConfigurationError(ErrorCode.RE1)
    assert e.value.code == ErrorCode.SDKE1.value.get("code")
    assert e.value.error_code == ErrorCode.SDKE1


def test_registration_configuration_error():
    with pytest.raises(RegistrationError) as e:
        raise RegistrationError(ErrorCode.RE1)
    assert e.value.code == ErrorCode.RE1.value.get("code")
    assert e.value.error_code == ErrorCode.RE1


def test_registration_configuration_error_wrong_code_fails():
    with pytest.raises(SDKError) as e:
        raise RegistrationError(ErrorCode.SOCE1, "dummy", "dummy", "dummy")
    assert e.value.code == ErrorCode.SDKE1.value.get("code")
    assert e.value.error_code == ErrorCode.SDKE1


def test_decorator_configuration_error():
    with pytest.raises(DecoratorConfigurationError) as e:
        raise DecoratorConfigurationError(ErrorCode.DCE1, "dummy")
    assert e.value.code == ErrorCode.DCE1.value.get("code")
    assert e.value.error_code == ErrorCode.DCE1


def test_decorator_configuration_error_wrong_code_fails():
    with pytest.raises(SDKError) as e:
        raise DecoratorConfigurationError(ErrorCode.SOCE1, "dummy", "dummy", "dummy")
    assert e.value.code == ErrorCode.SDKE1.value.get("code")
    assert e.value.error_code == ErrorCode.SDKE1
