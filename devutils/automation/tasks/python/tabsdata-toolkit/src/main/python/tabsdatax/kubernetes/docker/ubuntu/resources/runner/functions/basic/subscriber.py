#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


@td.subscriber(
    tables=["spanish", "french"],
    destination=td.LocalFileDestination([
        "/root/.td/functions/basic/output/spanish.jsonl",
        "/root/.td/functions/basic/output/french.jsonl",
    ]),
)
def persons_subscriber(
    spanish: td.TableFrame, french: td.TableFrame
) -> (td.TableFrame, td.TableFrame):
    return spanish, french
