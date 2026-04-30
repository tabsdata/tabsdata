#
# Copyright 2026 Tabs Data Inc.
#

import pytest

from tabsdata._utils.compatibility import (
    CompatibilityError,
    PackageVersionError,
    check_sticky_version_package,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


class TestCompatibilityError:
    def test_single_message(self):
        err = CompatibilityError("single error")
        assert err.messages == ["single error"]
        assert str(err) == "single error"

    def test_list_of_messages(self):
        err = CompatibilityError(["error 1", "error 2"])
        assert err.messages == ["error 1", "error 2"]
        assert "error 1" in str(err)
        assert "error 2" in str(err)

    def test_is_runtime_error(self):
        assert issubclass(CompatibilityError, RuntimeError)


class TestPackageVersionError:
    def test_single_message(self):
        err = PackageVersionError("version mismatch")
        assert err.messages == ["version mismatch"]
        assert str(err) == "version mismatch"

    def test_list_of_messages(self):
        err = PackageVersionError(["msg1", "msg2"])
        assert err.messages == ["msg1", "msg2"]

    def test_is_runtime_error(self):
        assert issubclass(PackageVersionError, RuntimeError)


class TestCheckStickyVersionPackage:
    def test_no_requires_returns_none(self):
        # None requires list should be a no-op
        check_sticky_version_package("polars", None)

    def test_empty_requires_returns_none(self):
        check_sticky_version_package("polars", [])

    def test_package_not_in_requires_returns_none(self):
        requires = ["other-package==1.0.0", "another==2.0.0"]
        check_sticky_version_package("polars", requires)

    def test_package_not_installed_raises(self):
        requires = ["nonexistent_package_12345==1.0.0"]
        with pytest.raises(PackageVersionError, match="not installed"):
            check_sticky_version_package("nonexistent_package_12345", requires)

    def test_matching_version_passes(self):
        import polars
        current_version = polars.__version__
        requires = [f"polars=={current_version}"]
        # Should not raise
        check_sticky_version_package("polars", requires)

    def test_mismatched_version_raises(self):
        requires = ["polars==0.0.0.fake"]
        with pytest.raises(PackageVersionError, match="version mismatch"):
            check_sticky_version_package("polars", requires)
