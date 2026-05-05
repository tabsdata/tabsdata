#
#  Copyright 2026 Tabs Data Inc.
#

import pytest

from tabsdatak.example.dummy import add, echo, ping


@pytest.mark.tabsdatak
def test_ping():
    assert ping() == "pong"


@pytest.mark.tabsdatak
def test_echo():
    assert echo("hello tabsdatak") == "hello tabsdatak"


@pytest.mark.tabsdatak
def test_add():
    assert add(2, 3) == 5
