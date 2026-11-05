#
# Copyright 2026 Tabsdata Inc.
#

import os

from tabsdatak.server._tdrunner.function._commons.monitor import sysinfo


def test_a_probe_the_platform_lacks_is_unavailable_not_an_error(monkeypatch, tmp_path):
    monkeypatch.delattr(os, "sched_getaffinity", raising=False)
    monkeypatch.setattr(sysinfo, "SYS_BLOCK", str(tmp_path / "missing"))
    report = sysinfo.SystemReport()
    report.record("usable_cpus", report.usable_cpus)
    report.record("block_devices", report.block_devices)
    assert report.failures == []
    assert report.lines == [
        f"usable_cpus = {sysinfo.UNAVAILABLE}",
        f"block_devices = {sysinfo.UNAVAILABLE}",
    ]


def test_an_affinity_the_platform_refuses_is_unavailable_not_an_error(monkeypatch):
    def refuse(_pid):
        raise OSError("not permitted")

    monkeypatch.setattr(os, "sched_getaffinity", refuse, raising=False)
    assert sysinfo.SystemReport.usable_cpus() == sysinfo.UNAVAILABLE
