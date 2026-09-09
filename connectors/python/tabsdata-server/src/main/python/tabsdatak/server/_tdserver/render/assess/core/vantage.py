#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from enum import Enum

from tabsdatak.server._tdserver.render.assess import State


class Tier(Enum):
    LOCAL = "local"
    CLUSTER = "cluster"
    REMOTE = "remote"

    @property
    def caption(self) -> str:
        return CAPTIONS[self]

    @property
    def unreachable_state(self) -> State:
        return State.WARNING if self is Tier.REMOTE else State.FAILED


CAPTIONS: dict[Tier, str] = {
    Tier.LOCAL: "Local",
    Tier.CLUSTER: "Cluster",
    Tier.REMOTE: "Remote",
}


class Layer(Enum):
    RESOLVE = "resolve"
    CONNECT = "connect"
    AUTHENTICATE = "authenticate"
    ASSERT = "assert"

    @property
    def reached(self) -> bool:
        return self in (Layer.AUTHENTICATE, Layer.ASSERT)


def state_for(tier: Tier, layer: Layer, satisfied: bool) -> State:
    if satisfied:
        return State.PASSED
    if tier is not Tier.REMOTE:
        return State.FAILED
    return State.FAILED if layer.reached else State.WARNING
