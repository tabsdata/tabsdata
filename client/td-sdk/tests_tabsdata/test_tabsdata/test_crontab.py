#
# Copyright 2025 Tabs Data Inc.
#

from datetime import datetime

import pytest
from dateutil import tz

from tabsdata._cron_trigger import CronTrigger
from tabsdata.exceptions import (
    DecoratorConfigurationError,
    ErrorCode,
)

EXAMPLE_START_DATETIME = datetime(2025, 4, 2, tzinfo=tz.UTC)
EXAMPLE_END_DATETIME = datetime(2025, 4, 3, tzinfo=tz.UTC)


@pytest.mark.unit
@pytest.mark.crontab
def test_crontab_class_defaults():
    crontab = CronTrigger(mask="25 19 * * 1,3,5")
    assert crontab.mask == "25 19 * * 1,3,5"
    assert crontab.start is None
    assert crontab.end is None
    assert crontab._to_dict() == {"mask": "25 19 * * 1,3,5", "start": None, "end": None}


@pytest.mark.unit
@pytest.mark.crontab
def test_crontab_class_all_params():
    crontab = CronTrigger(
        "25 19 * * 1,3,5", EXAMPLE_START_DATETIME, EXAMPLE_END_DATETIME
    )
    assert crontab.mask == "25 19 * * 1,3,5"
    assert crontab.start == EXAMPLE_START_DATETIME.isoformat(timespec="microseconds")
    assert crontab.end == EXAMPLE_END_DATETIME.isoformat(timespec="microseconds")
    assert crontab._to_dict() == {
        "mask": "25 19 * * 1,3,5",
        "start": EXAMPLE_START_DATETIME.isoformat(timespec="microseconds"),
        "end": EXAMPLE_END_DATETIME.isoformat(timespec="microseconds"),
    }


@pytest.mark.unit
@pytest.mark.crontab
def test_crontab_wrong_mask_element_counts():
    with pytest.raises(DecoratorConfigurationError) as e:
        # noinspection PyTypeChecker
        CronTrigger(
            mask="1 * * *",
        )
    assert e.value.error_code == ErrorCode.DCE14

    with pytest.raises(DecoratorConfigurationError) as e:
        # noinspection PyTypeChecker
        CronTrigger(
            mask="* 1 * * * *",
        )
    assert e.value.error_code == ErrorCode.DCE14


@pytest.mark.unit
@pytest.mark.crontab
def test_crontab_wrong_start_value():
    with pytest.raises(DecoratorConfigurationError) as e:
        # noinspection PyTypeChecker
        CronTrigger(mask="25 19 * * 1,3,5", start="2")
    assert e.value.error_code == ErrorCode.DCE11


@pytest.mark.unit
@pytest.mark.crontab
def test_crontab_wrong_end_value():
    with pytest.raises(DecoratorConfigurationError) as e:
        # noinspection PyTypeChecker
        CronTrigger(mask="25 19 * * 1,3,5", end="2")
    assert e.value.error_code == ErrorCode.DCE12


@pytest.mark.unit
@pytest.mark.crontab
def test_crontab_mask_validation_success():
    crontab = CronTrigger(mask="*/5 * * * *")
    assert crontab.mask == "*/5 * * * *"


@pytest.mark.unit
@pytest.mark.crontab
def test_crontab_mask_validation_failure():
    with pytest.raises(DecoratorConfigurationError) as e:
        CronTrigger(mask="A * * * *")
    assert e.value.error_code == ErrorCode.DCE15

    with pytest.raises(DecoratorConfigurationError) as e:
        CronTrigger(mask=None)
    assert e.value.error_code == ErrorCode.DCE16

    with pytest.raises(DecoratorConfigurationError) as e:
        CronTrigger(mask=1)
    assert e.value.error_code == ErrorCode.DCE16

    with pytest.raises(DecoratorConfigurationError) as e:
        CronTrigger(mask="@hourly")
    assert e.value.error_code == ErrorCode.DCE19
