#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.server._tdserver.render.assess import State
from tabsdatak.server._tdserver.render.assess.core.vantage import Layer, Tier, state_for

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("tier", list(Tier))
@pytest.mark.parametrize("layer", list(Layer))
def test_satisfied_always_passes(tier, layer):
    assert state_for(tier, layer, True) is State.PASSED


@pytest.mark.parametrize("tier", [Tier.LOCAL, Tier.CLUSTER])
@pytest.mark.parametrize("layer", list(Layer))
def test_local_and_cluster_always_fail(tier, layer):
    assert state_for(tier, layer, False) is State.FAILED


@pytest.mark.parametrize("layer", [Layer.RESOLVE, Layer.CONNECT])
def test_remote_warns_when_it_never_reached_the_target(layer):
    assert state_for(Tier.REMOTE, layer, False) is State.WARNING


@pytest.mark.parametrize("layer", [Layer.AUTHENTICATE, Layer.ASSERT])
def test_remote_fails_once_it_reached_the_target(layer):
    assert state_for(Tier.REMOTE, layer, False) is State.FAILED


def test_only_remote_degrades_when_unreachable():
    assert Tier.LOCAL.unreachable_state is State.FAILED
    assert Tier.CLUSTER.unreachable_state is State.FAILED
    assert Tier.REMOTE.unreachable_state is State.WARNING


def test_reached_layers_are_the_ones_past_a_connection():
    assert not Layer.RESOLVE.reached
    assert not Layer.CONNECT.reached
    assert Layer.AUTHENTICATE.reached
    assert Layer.ASSERT.reached
