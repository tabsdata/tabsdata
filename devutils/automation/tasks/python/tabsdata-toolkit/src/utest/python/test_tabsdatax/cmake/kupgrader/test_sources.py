#
# Copyright 2026 Tabsdata Inc.
#

import pytest

from tabsdatax.cmake.kupgrader.common.sources import (
    PATCHES_SUBPATH,
    SourceError,
    apply_published_patch,
)

PATCH = """--- a/configs/tabsdata.yaml
+++ b/configs/tabsdata.yaml
@@ -1 +1 @@
-published: false
+published: true
"""


def kprofile_in(folder):
    (folder / "configs").mkdir(parents=True)
    (folder / "configs" / "tabsdata.yaml").write_text("published: false\n")
    return folder


def patch_in(root, version, content):
    patches = root / PATCHES_SUBPATH
    patches.mkdir(parents=True)
    (patches / f"{version}.patch").write_text(content)


def test_a_published_patch_is_applied(tmp_path):
    kprofile = kprofile_in(tmp_path / "kprofile")
    patch_in(tmp_path, "2.0.0", PATCH)

    apply_published_patch(tmp_path, "2.0.0", kprofile)

    assert (kprofile / "configs" / "tabsdata.yaml").read_text() == "published: true\n"


def test_a_version_without_patch_is_left_alone(tmp_path):
    kprofile = kprofile_in(tmp_path / "kprofile")
    patch_in(tmp_path, "2.0.0", PATCH)

    apply_published_patch(tmp_path, "2.0.1", kprofile)

    assert (kprofile / "configs" / "tabsdata.yaml").read_text() == "published: false\n"


def test_a_patch_that_does_not_apply_fails(tmp_path):
    kprofile = kprofile_in(tmp_path / "kprofile")
    patch_in(tmp_path, "2.0.0", PATCH.replace("published: false", "published: never"))

    with pytest.raises(SourceError):
        apply_published_patch(tmp_path, "2.0.0", kprofile)
