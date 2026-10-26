#
# Copyright 2026 Tabsdata Inc.
#

from tabsdatax.cmake.kupgrader.common import ownership
from tabsdatax.cmake.kupgrader.common.ownership import ownership_problems


def test_the_ownership_problems_come_from_the_upgrader_binary(tmp_path, monkeypatch):
    calls = []
    problem = (
        "The upgrader of version '2.3.0' changes the open mapping "
        "'storage.mounts.ROOT.options' of 'configs/apiserver.storage.yaml'"
    )

    def run_upgrader(binary, arguments):
        calls.append((binary, arguments))
        return {"problems": [problem]}

    monkeypatch.setattr(ownership, "run_upgrader", run_upgrader)
    binary = tmp_path / "tdk_kupgrader"
    problems = ownership_problems(
        binary,
        tmp_path / "kupgrade",
        "2.3.0",
        tmp_path / "previous",
        tmp_path / "current",
    )
    assert problems == [problem]
    assert calls == [
        (
            binary,
            [
                "ownership",
                "--kupgrade",
                str(tmp_path / "kupgrade"),
                "--version",
                "2.3.0",
                "--previous",
                str(tmp_path / "previous"),
                "--current",
                str(tmp_path / "current"),
            ],
        )
    ]
