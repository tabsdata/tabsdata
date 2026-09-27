#
# Copyright 2026 Tabsdata Inc.
#

import yaml

from tabsdatax.cmake.kupgrader.common.emission import Emission
from tabsdatax.cmake.kupgrader.common.templates import (
    Mismatch,
    mismatches_of,
    stage_kupgrade,
)
from tabsdatax.cmake.kupgrader.common.versions import Entry, Resolution
from tabsdatax.cmake.kupgrader.common.workflow import context_of


def shipped_tree(root):
    upgrades = root / "variant" / "resources" / "kupgrade" / "upgrades"
    (upgrades / "2.0.1").mkdir(parents=True)
    (upgrades / "2.0.1.yaml").write_text("schema: 1\n")
    (upgrades / "2.0.2.yaml").write_text("schema: 1\nstale: true\n")
    (upgrades / "2.0.2").mkdir()
    (upgrades / "2.0.2" / "stale.yaml").write_text("stale\n")
    (upgrades.parent / "upgrades.yaml").write_text(
        "schema: 1\nversions:\n"
        "  - version: 2.0.1\n    upgrader: true\n"
        "  - version: 2.0.0\n    upgrader: false\n"
    )


def emission_in(folder):
    content = folder / "2.0.2"
    (content / "configs").mkdir(parents=True)
    (content / "configs" / "fresh.yaml").write_text("fresh\n")
    spec = folder / "2.0.2.yaml"
    spec.write_text("schema: 1\nfresh: true\n")
    return Emission(spec_file=spec, content_folder=content, payloads=[])


def test_staging_overlays_the_generated_upgrader_on_the_shipped_tree(tmp_path):
    shipped_tree(tmp_path)
    context = context_of("build", tmp_path)
    emission = emission_in(tmp_path / "generated")
    resolution = Resolution(
        current="2.0.2",
        previous="2.0.1",
        entries=[
            Entry(version="2.0.1", upgrader=True),
            Entry(version="2.0.0", upgrader=False),
        ],
    )

    staged = stage_kupgrade(context, emission, resolution)

    upgrades = staged / "upgrades"
    assert (upgrades / "2.0.1.yaml").is_file()
    assert yaml.safe_load((upgrades / "2.0.2.yaml").read_text()) == {
        "schema": 1,
        "fresh": True,
    }
    assert (upgrades / "2.0.2" / "configs" / "fresh.yaml").is_file()
    assert not (upgrades / "2.0.2" / "stale.yaml").exists()
    inventory = yaml.safe_load((staged / "upgrades.yaml").read_text())
    assert inventory["versions"][0] == {"version": "2.0.2", "upgrader": True}
    shipped = context.kupgrade_folder / "upgrades" / "2.0.2.yaml"
    assert "stale" in shipped.read_text()


def test_a_report_without_mismatches_holds_none():
    assert mismatches_of({"mismatches": []}) == []


def test_a_report_lists_its_mismatches():
    report = {
        "mismatches": [
            {
                "provider": "kminus",
                "version": "2.0.2",
                "file": "pods/cron-publisher.yaml",
                "reason": "differs",
            }
        ]
    }
    assert mismatches_of(report) == [
        Mismatch(
            provider="kminus",
            version="2.0.2",
            file="pods/cron-publisher.yaml",
            reason="differs",
        )
    ]
