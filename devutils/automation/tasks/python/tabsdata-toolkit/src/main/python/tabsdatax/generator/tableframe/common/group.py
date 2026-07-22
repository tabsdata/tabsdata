#
# Copyright 2026 Tabsdata Inc.
#

import rich_click as click


class OrderedGroup(click.RichGroup):
    def list_commands(self, ctx: click.Context) -> list[str]:
        return list(self.commands)
