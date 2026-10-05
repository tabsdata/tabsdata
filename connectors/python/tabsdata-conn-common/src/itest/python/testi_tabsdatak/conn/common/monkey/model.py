#
# Copyright 2026 Tabsdata Inc.
#

"""The state a monkey run should leave behind, kept from the operations the
server accepted. Whether an operation is accepted can depend on timing (a
cancel racing a finishing run) or on other functions (registering a
transformer whose input table is gone), so the model follows the outcomes
instead of predicting them; what it predicts is only what has a single right
answer: an operation on a function that does not exist, or a trigger of a
disabled one, is rejected."""

from __future__ import annotations

import threading
from dataclasses import dataclass

from testi_tabsdatak.conn.common.monkey.plan import (
    DELETE,
    DISABLE,
    ENABLE,
    REGISTER,
    TRIGGER,
    UPDATE,
    Fleet,
    Op,
)

OK = "ok"
REJECTED = "rejected"
SKIPPED = "skipped"
ERROR = "error"

ANY = "any"

NEEDS_FUNCTION = (DELETE, DISABLE, ENABLE, TRIGGER, UPDATE)


@dataclass
class FunctionState:
    exists: bool = True
    enabled: bool = True


class Model:
    def __init__(self, fleet: Fleet):
        self._lock = threading.Lock()
        self._states = {spec.name: FunctionState() for spec in fleet.functions}

    def predict(self, op: Op) -> str:
        if op.function is None:
            return ANY
        with self._lock:
            state = self._states[op.function]
            if op.action in NEEDS_FUNCTION and not state.exists:
                return REJECTED
            if op.action == REGISTER and state.exists:
                return REJECTED
            if op.action == TRIGGER and not state.enabled:
                return REJECTED
            return ANY

    def enabled(self, function: str) -> bool:
        with self._lock:
            return self._states[function].enabled

    def apply(self, op: Op, result: str) -> None:
        if result != OK or op.function is None:
            return
        with self._lock:
            state = self._states[op.function]
            if op.action == REGISTER:
                state.exists, state.enabled = True, True
            elif op.action == DELETE:
                state.exists = False
            elif op.action == ENABLE:
                state.enabled = True
            elif op.action == DISABLE:
                state.enabled = False

    def expected(self) -> dict[str, dict]:
        with self._lock:
            return {
                name: {"exists": state.exists, "enabled": state.enabled}
                if state.exists
                else {"exists": False}
                for name, state in self._states.items()
            }
