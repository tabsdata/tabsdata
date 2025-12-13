#
# Copyright 2025 Tabs Data Inc.
#

from datetime import datetime

import pytest

from tabsdata._io.watchdog import WatchDog
from tabsdata.exceptions import (
    ErrorCode,
    WatchDogConfigurationError,
)

EXAMPLE_START_DATETIME = datetime(2025, 4, 2)
EXAMPLE_END_DATETIME = datetime(2025, 4, 3)


@pytest.mark.unit
@pytest.mark.watchdog
def test_watchdog_class_defaults():
    watchdog = WatchDog()
    assert watchdog.timeout_mins is None
    assert watchdog.size is None
    assert watchdog.start is None
    assert watchdog.end is None
    assert watchdog._to_dict() == {
        "timeout_mins": None,
        "size": None,
        "start": None,
        "end": None,
    }


@pytest.mark.unit
@pytest.mark.watchdog
def test_watchdog_class_all_params():
    watchdog = WatchDog(2, 7, EXAMPLE_START_DATETIME, EXAMPLE_END_DATETIME)
    assert watchdog.timeout_mins == 2
    assert watchdog.size == 7
    assert watchdog.start == EXAMPLE_START_DATETIME
    assert watchdog.end == EXAMPLE_END_DATETIME
    assert watchdog._to_dict() == {
        "timeout_mins": 2,
        "size": 7,
        "start": EXAMPLE_START_DATETIME.isoformat(),
        "end": EXAMPLE_END_DATETIME.isoformat(),
    }


@pytest.mark.unit
@pytest.mark.watchdog
def test_watchdog_wrong_timeout_type():
    with pytest.raises(WatchDogConfigurationError) as e:
        WatchDog(timeout_mins="2")
    assert e.value.error_code == ErrorCode.WCE1


@pytest.mark.unit
@pytest.mark.watchdog
def test_watchdog_wrong_timeout_value():
    with pytest.raises(WatchDogConfigurationError) as e:
        WatchDog(timeout_mins=-2)
    assert e.value.error_code == ErrorCode.WCE1


@pytest.mark.unit
@pytest.mark.watchdog
def test_watchdog_wrong_size_type():
    with pytest.raises(WatchDogConfigurationError) as e:
        WatchDog(size="2")
    assert e.value.error_code == ErrorCode.WCE2


@pytest.mark.unit
@pytest.mark.watchdog
def test_watchdog_wrong_size_value():
    with pytest.raises(WatchDogConfigurationError) as e:
        WatchDog(size=-2)
    assert e.value.error_code == ErrorCode.WCE2


@pytest.mark.unit
@pytest.mark.watchdog
def test_watchdog_wrong_start_value():
    with pytest.raises(WatchDogConfigurationError) as e:
        WatchDog(start="2")
    assert e.value.error_code == ErrorCode.WCE3


@pytest.mark.unit
@pytest.mark.watchdog
def test_watchdog_wrong_end_value():
    with pytest.raises(WatchDogConfigurationError) as e:
        WatchDog(end="2")
    assert e.value.error_code == ErrorCode.WCE4
