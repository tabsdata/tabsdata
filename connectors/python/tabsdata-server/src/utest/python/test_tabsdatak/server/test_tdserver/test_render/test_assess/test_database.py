#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.server._tdserver.render.assess import State
from tabsdatak.server._tdserver.render.assess.checks.database import (
    answered,
    judged,
    server_of,
    settled,
    spelled,
    spoken,
)

pytestmark = pytest.mark.unit

SERVER = """
database_server:
  postgres:
    host: db.example
    port: 5432
    database: probe
    username: probe
    password: secret
"""

PENDING = """
database_server:
  postgres:
    host: ${{literal:tdk_database_host}}
    port: ${{literal:tdk_database_port}}
    database: ${{literal:tdk_database_name}}
    username: ${{secret:postgres_system_user}}
    password: ${{secret:postgres_system_password}}
"""


def test_a_filled_config_yields_a_server(prepared):
    prepared("probe", configs={"dbmanager.database.yaml": SERVER})
    found = server_of("probe")
    assert found.host == "db.example"
    assert found.port == 5432
    assert found.database == "probe"
    assert found.username == "probe"


def test_a_config_still_holding_placeholders_yields_nothing(prepared):
    prepared("probe", configs={"dbmanager.database.yaml": PENDING})
    assert server_of("probe") is None


def test_a_missing_config_yields_nothing(prepared):
    prepared("probe")
    assert server_of("probe") is None


def test_a_port_that_is_not_a_number_yields_nothing(prepared):
    prepared(
        "probe",
        configs={"dbmanager.database.yaml": SERVER.replace("5432", "later")},
    )
    assert server_of("probe") is None


def test_an_inline_certificate_travels_with_the_server(prepared):
    prepared(
        "probe",
        configs={
            "dbmanager.database.yaml": SERVER + "    ssl_root_cert_pem: |\n      pem\n"
        },
    )
    assert server_of("probe").certificate.strip() == "pem"


def test_a_server_without_a_certificate_carries_none(prepared):
    prepared("probe", configs={"dbmanager.database.yaml": SERVER})
    assert server_of("probe").certificate is None


def test_an_unfilled_placeholder_counts_as_no_value():
    assert settled("${{secret:postgres_system_user}}") == ""
    assert settled("d_probe") == "d_probe"
    assert settled(None) == ""


def test_a_postgres_answer_is_told_from_a_defect_of_our_own():
    said = Exception({"S": "FATAL", "M": "password authentication failed"})
    assert answered(said)
    assert spoken(said) == "password authentication failed"


def test_a_defect_of_our_own_keeps_its_own_words():
    said = RuntimeError("attempting to use unexecuted cursor")
    assert not answered(said)
    assert spoken(said) == "attempting to use unexecuted cursor"


@pytest.mark.parametrize("found", [170_004, 160_000, 90_600])
def test_a_major_below_the_supported_one_is_refused(found):
    state, title, _ = judged(found)
    assert state is State.FAILED
    assert "older" in title


@pytest.mark.parametrize("found", [190_000, 200_001])
def test_a_major_above_the_supported_one_is_flagged(found):
    state, title, _ = judged(found)
    assert state is State.WARNING
    assert "newer" in title


@pytest.mark.parametrize("found", [180_000, 180_003, 180_005, 180_006])
def test_an_untested_minor_of_the_supported_major_is_flagged(found):
    state, title, _ = judged(found)
    assert state is State.WARNING
    assert "not tested" in title


def test_the_tested_version_passes_without_comment():
    state, title, guidance = judged(180_004)
    assert state is State.PASSED
    assert not title and not guidance


@pytest.mark.parametrize(
    ("number", "spoken_as"),
    [(180_004, "18.4"), (170_000, "17.0"), (90_600, "9.600")],
)
def test_a_version_number_is_spelled_as_people_write_it(number, spoken_as):
    assert spelled(number) == spoken_as
