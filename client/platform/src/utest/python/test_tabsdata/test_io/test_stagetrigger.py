#
# Copyright 2025 Tabsdata Inc.
#

from datetime import datetime, timezone

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

EXAMPLE_START_DATETIME = datetime(2025, 4, 2, tzinfo=timezone.utc)
EXAMPLE_END_DATETIME = datetime(2025, 4, 3, tzinfo=timezone.utc)


class MyStageTrigger(StageTrigger):

    def run(self, ctx: StageTriggerContext) -> StageTriggerStatus:
        pass

    def _to_dict(self) -> dict:
        d = super()._to_dict()
        d["foo"] = "bar"
        return d


@pytest.mark.unit
@pytest.mark.stagetrigger
def test_stagetrigger_class_defaults():
    stagetrigger = MyStageTrigger()
    assert stagetrigger.start is None
    assert stagetrigger.end is None
    assert stagetrigger._to_dict() == {"start": None, "end": None, "foo": "bar"}


@pytest.mark.unit
@pytest.mark.stagetrigger
def test_stagetrigger_class_all_params():
    stagetrigger = MyStageTrigger(EXAMPLE_START_DATETIME, EXAMPLE_END_DATETIME)
    assert stagetrigger.start == EXAMPLE_START_DATETIME
    assert stagetrigger.end == EXAMPLE_END_DATETIME
    assert stagetrigger._to_dict() == {
        "start": EXAMPLE_START_DATETIME.isoformat(),
        "end": EXAMPLE_END_DATETIME.isoformat(),
        "foo": "bar",
    }


@pytest.mark.unit
@pytest.mark.stagetrigger
def test_stagetrigger_wrong_start_value():
    with pytest.raises(StageTriggerConfigurationError) as e:
        MyStageTrigger(start="2")
    assert e.value.error_code == ErrorCode.STE1


@pytest.mark.unit
@pytest.mark.stagetrigger
def test_stagetrigger_wrong_end_value():
    with pytest.raises(StageTriggerConfigurationError) as e:
        MyStageTrigger(end="2")
    assert e.value.error_code == ErrorCode.STE2
