#
#  Copyright 2026 Tabs Data Inc.
#


def tabsdata_command(func=None, *, name=None):
    def wrap(f):
        f.tabsdata_command = True
        f.tabsdata_command_name = name
        return f

    return wrap if func is None else wrap(func)
