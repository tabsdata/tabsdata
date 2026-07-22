#
# Copyright 2026 Tabsdata Inc.
#

import pytest
import yaml

from tabsdata._tabsserver.tools.server_info import resolve_valid_python_versions

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401


class TestServerInfoLoadVersions:
    def test_load_versions_with_valid_file(self, tmp_path):
        """Verify resolve_valid_python_versions reads correctly with context managers."""
        available = tmp_path / "available.yaml"
        allowed = tmp_path / "allowed.yaml"
        available.write_text(yaml.dump({"versions": ["3.12.0", "3.11.5", "3.10.0"]}))
        allowed.write_text(yaml.dump({"versions": ["3.12.0", "3.11.5"]}))

        resolve_valid_python_versions(
            etc=str(tmp_path),
            available_python_versions_file="available.yaml",
            allowed_python_versions_file="allowed.yaml",
            valid_python_versions_file="valid_versions.yaml",
        )

        output_file = tmp_path / "valid_versions.yaml"
        assert output_file.exists()
        with open(output_file) as f:
            content = yaml.safe_load(f)
        versions = content.get("versions")
        assert isinstance(versions, list)
        assert len(versions) > 0
        for v in versions:
            assert v in ["3.12.0", "3.11.5"]

    def test_load_versions_missing_file(self, tmp_path):
        """Verify missing version files don't crash."""
        resolve_valid_python_versions(
            etc=str(tmp_path),
            available_python_versions_file="nonexistent_available.yaml",
            allowed_python_versions_file="nonexistent_allowed.yaml",
            valid_python_versions_file="valid_versions.yaml",
        )

        output_file = tmp_path / "valid_versions.yaml"
        assert output_file.exists()
        with open(output_file) as f:
            content = yaml.safe_load(f)
        assert content.get("versions") == []
