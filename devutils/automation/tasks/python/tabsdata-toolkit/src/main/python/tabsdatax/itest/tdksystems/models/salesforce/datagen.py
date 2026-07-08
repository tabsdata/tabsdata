#!/usr/bin/env python3
"""Salesforce — Accounts + Opportunities data.

This is the *salesforce* half of the Salesforce ↔ Finance pair. It is loaded
straight into the org by `tdx-domain create --model salesforce --data` (which
wipes + bulk-uploads via tabsdatax.itest.tdksystems.salesforce_loader, using the bundled
recipe + mapping shipped alongside this stub), not through the Table-emit
pipeline — so this file's ``main()`` is just a defensive fallback for direct
invocation. See SALESFORCE.md.

Companion model ``finance`` joins back to the Accounts loaded here via
``customers.salesforce_account_id``.
"""

from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
README = HERE / "SALESFORCE.md"


def main() -> None:
    print(
        "ERROR: the 'salesforce' model is loaded straight into the org, not via\n"
        "       the generic data pipeline. Run:\n"
        "           tdx-domain create --model salesforce --data <SIZE> ...\n"
        f"       See {README}.",
        file=sys.stderr,
    )
    raise SystemExit(2)


if __name__ == "__main__":
    main()
