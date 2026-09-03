#
#  Copyright 2026 Tabsdata Inc.
#

import importlib
import sys

MODULES: tuple[str, ...] = (
    "lancedb",
    "fastembed",
    "onnxruntime",
    "pandas",
    "pyarrow",
    "polars",
    "tabsdatak._connectors",
    "tabsdatak.server",
    "tabsdatak.ag.entry",
)


def main() -> int:
    for module in MODULES:
        try:
            importlib.import_module(module)
        except BaseException as error:  # noqa: BLE001 — Warming is best effort
            print(f"Skipped {module}: {error}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
