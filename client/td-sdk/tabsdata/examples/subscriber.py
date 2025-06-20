import os

from verify_environment import verify_subscriber_environment

import tabsdata as td

verify_subscriber_environment()


@td.subscriber(
    ["spanish", "french"],
    td.LocalFileDestination(
        [
            os.path.join(os.getenv("TDX"), "output", "spanish.jsonl"),
            os.path.join(os.getenv("TDX"), "output", "french.jsonl"),
        ]
    ),
)
def sub(
    spanish: td.TableFrame, french: td.TableFrame
) -> (td.TableFrame, td.TableFrame):
    return spanish, french
