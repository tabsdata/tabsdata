#
# Copyright 2025 Tabs Data Inc.
#

import tabsdata as td
from tabsdata._io.watchdog import SourceListener, SourceListenerContext, WatchDog

# In this example, we are obtaining the data from two tables in the database.
# INVOICE_HEADER contains the following data:
# [{"id":1,"name":"Arvind"},{"id":2,"name":"Tucu"},{"id":3,"name":"Dimas"},
# {"id":4,"name":"Joaquin"},{"id":5,"name":"Jennifer"},{"id":6,"name":"Aleix"}]
# INVOICE_ITEM contains the following data:
# [{"id":1,"name":"Leonardo"},{"id":2,"name":"Donatello"},{"id":3,
# "name":"Michelangelo"},{"id":4,"name":"Raphael"},{"id":5,"name":"Splinter"}]


class FakeSourcePlugin(td.SourcePlugin):
    def __init__(self):
        pass

    def allows_listener(self) -> bool:
        return True

    @property
    def listener(self) -> SourceListener:
        return SampleWatchdogRunner(self._injected_watchdog)


class SampleWatchdogRunner(SourceListener):

    def __init__(self, watchdog: WatchDog):
        super().__init__(watchdog)

    def listen(self, ctx: SourceListenerContext):
        assert isinstance(ctx, SourceListenerContext)
        assert ctx.trigger_config.url
        assert ctx.state_config.url
        assert ctx.status_config.url
        return True


def testing_tabsdata_function():

    @td.publisher(
        trigger_by=WatchDog(),
        name="test_input_watchdog",
        source=FakeSourcePlugin(),
        tables=["output1", "output2"],  # required,
    )
    def input_watchdog(
        headers: td.TableFrame, items: td.TableFrame
    ) -> (td.TableFrame, td.TableFrame):
        # transformations can be done here
        return headers, items

    return input_watchdog
