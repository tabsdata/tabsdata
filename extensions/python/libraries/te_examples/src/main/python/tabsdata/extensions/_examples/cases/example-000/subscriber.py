import os

import tabsdata as td


@td.subscriber(
    tables=["spanish", "french"],
    destination=td.LocalFileDestination(
        [
            os.path.join(os.getcwd(), "output", "spanish.jsonl"),
            os.path.join(os.getcwd(), "output", "french.jsonl"),
        ]
    ),
)
def sub(
    spanish: td.TableFrame, french: td.TableFrame
) -> (td.TableFrame, td.TableFrame):
    return spanish, french
