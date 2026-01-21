#
# Copyright 2025 Tabs Data Inc.
#

from datetime import datetime

import pytest

from tabsdata._io.stage_trigger import (
    StageTrigger,
    StageTriggerContext,
    StageTriggerStatus,
)
from tabsdata.exceptions import (
    ErrorCode,
    StageTriggerConfigurationError,
)

EXAMPLE_START_DATETIME = datetime(2025, 4, 2)
EXAMPLE_END_DATETIME = datetime(2025, 4, 3)


class MyStageTrigger(StageTrigger):

    def run(self, ctx: StageTriggerContext) -> StageTriggerStatus:
        pass

    def _to_dict(self) -> dict:
        d = super()._to_dict()
        d["foo"] = "bar"
        return d


@pytest.mark.unit
@pytest.mark.watchdog
def test_watchdog_class_defaults():
    watchdog = MyStageTrigger()
    assert watchdog.start is None
    assert watchdog.end is None
    assert watchdog._to_dict() == {"start": None, "end": None, "foo": "bar"}


@pytest.mark.unit
@pytest.mark.watchdog
def test_watchdog_class_all_params():
    watchdog = MyStageTrigger(EXAMPLE_START_DATETIME, EXAMPLE_END_DATETIME)
    assert watchdog.start == EXAMPLE_START_DATETIME
    assert watchdog.end == EXAMPLE_END_DATETIME
    assert watchdog._to_dict() == {
        "start": EXAMPLE_START_DATETIME.isoformat(),
        "end": EXAMPLE_END_DATETIME.isoformat(),
        "foo": "bar",
    }


@pytest.mark.unit
@pytest.mark.watchdog
def test_watchdog_wrong_start_value():
    with pytest.raises(StageTriggerConfigurationError) as e:
        MyStageTrigger(start="2")
    assert e.value.error_code == ErrorCode.STE1


@pytest.mark.unit
@pytest.mark.watchdog
def test_watchdog_wrong_end_value():
    with pytest.raises(StageTriggerConfigurationError) as e:
        MyStageTrigger(end="2")
    assert e.value.error_code == ErrorCode.STE2
