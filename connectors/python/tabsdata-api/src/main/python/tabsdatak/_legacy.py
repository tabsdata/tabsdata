#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import importlib
import warnings
from datetime import datetime
from typing import Any, Callable

from typing_extensions import deprecated

from tabsdatak._annotations import deprecated_category
from tabsdatak._api import path_timestamp

SINCE = "2.3.0"
REASON = "Renamed to match the names of the Tabsdata server API."


def category(replacement: str) -> type[Warning]:
    return deprecated_category(reason=REASON, replacement=replacement, since=SINCE)


class FlowCtxLegacy:
    collection: str
    function: str

    @property
    @deprecated("Use 'collection' instead.", category=category("collection"))
    def collection_name(self) -> str:
        return self.collection

    @property
    @deprecated("Use 'function' instead.", category=category("function"))
    def function_name(self) -> str:
        return self.function


class TransactionCtxLegacy:
    execution_id: str
    transaction_id: str
    triggered_on: datetime

    @property
    @deprecated("Use 'execution_id' instead.", category=category("execution_id"))
    def exec_plan_id(self) -> str:
        return self.execution_id

    @property
    @deprecated("Use 'transaction_id' instead.", category=category("transaction_id"))
    def trx_id(self) -> str:
        return self.transaction_id

    @property
    @deprecated("Use 'triggered_on' instead.", category=category("triggered_on"))
    def exec_plan_ts(self) -> str:
        return path_timestamp(self.triggered_on)


def warn_renamed(old: str, replacement: str, stacklevel: int = 3) -> None:
    warnings.warn(
        f"'{old}' is deprecated; use '{replacement}' instead.",
        category=category(replacement),
        stacklevel=stacklevel,
    )


def module_getattr(
    module: str, renamed: dict[str, tuple[str, str]]
) -> Callable[[str], Any]:
    def __getattr__(name: str) -> Any:
        if name not in renamed:
            raise AttributeError(f"module '{module}' has no attribute '{name}'")
        target_module, target = renamed[name]
        warn_renamed(f"{module}.{name}", f"{target_module}.{target}")
        return getattr(importlib.import_module(target_module), target)

    return __getattr__
