#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import pytest
import yaml
from pydantic import ValidationError

from tabsdatak.server._tdrunner.function._lib.run_env import RunEnv, write_run_env

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- construction / defaults ---


def test_default_extra_env_is_empty():
    env = RunEnv(
        python_bin="/v/bin/python",
        venv_dir="/v",
        requirements_digest="sha256:abc",
    )
    assert env.extra_env == {}


# --- write produces the expected YAML shape ---


def test_write_minimal_yaml_shape(tmp_path):
    env = RunEnv(
        python_bin="/v/bin/python",
        venv_dir="/v",
        requirements_digest="sha256:abc",
    )
    out = tmp_path / "run-env.yaml"
    write_run_env(env, out)
    parsed = yaml.safe_load(out.read_text())
    assert parsed == {
        "python_bin": "/v/bin/python",
        "venv_dir": "/v",
        "requirements_digest": "sha256:abc",
        "extra_env": {},
    }


def test_write_with_extra_env(tmp_path):
    env = RunEnv(
        python_bin="/v/bin/python",
        venv_dir="/v",
        requirements_digest="sha256:abc",
        extra_env={"PYTHONPATH": "/foo:/bar", "TZ": "UTC"},
    )
    out = tmp_path / "run-env.yaml"
    write_run_env(env, out)
    parsed = yaml.safe_load(out.read_text())
    assert parsed["extra_env"] == {"PYTHONPATH": "/foo:/bar", "TZ": "UTC"}


def test_write_produces_plain_yaml(tmp_path):
    """No custom YAML tags — pod-runner reads this with stock yaml."""
    env = RunEnv(
        python_bin="/v/bin/python",
        venv_dir="/v",
        requirements_digest="sha256:abc",
    )
    out = tmp_path / "run-env.yaml"
    write_run_env(env, out)
    text = out.read_text()
    assert "!" not in text


# --- construction errors (pydantic raises on RunEnv(...)) ---


@pytest.mark.parametrize("missing", ["python_bin", "venv_dir", "requirements_digest"])
def test_missing_required_field(missing):
    fields = {
        "python_bin": "/v/bin/python",
        "venv_dir": "/v",
        "requirements_digest": "sha256:abc",
    }
    fields.pop(missing)
    with pytest.raises(ValidationError, match=missing):
        RunEnv(**fields)


def test_requirements_digest_missing_prefix():
    with pytest.raises(ValidationError, match="sha256:"):
        RunEnv(
            python_bin="/v/bin/python",
            venv_dir="/v",
            requirements_digest="abc",
        )


def test_python_bin_wrong_type():
    with pytest.raises(ValidationError, match="python_bin"):
        RunEnv(
            python_bin=42,  # type: ignore[arg-type]
            venv_dir="/v",
            requirements_digest="sha256:abc",
        )


def test_extra_env_value_wrong_type():
    with pytest.raises(ValidationError, match="extra_env"):
        RunEnv(
            python_bin="/v/bin/python",
            venv_dir="/v",
            requirements_digest="sha256:abc",
            extra_env={"KEY": [1, 2]},  # type: ignore[dict-item]
        )


def test_extra_field_is_forbidden():
    with pytest.raises(ValidationError, match="surprise"):
        RunEnv(
            python_bin="/v/bin/python",
            venv_dir="/v",
            requirements_digest="sha256:abc",
            surprise="x",  # type: ignore[call-arg]
        )


# --- write errors ---


def test_write_to_missing_parent_raises_oserror(tmp_path):
    env = RunEnv(
        python_bin="/v/bin/python",
        venv_dir="/v",
        requirements_digest="sha256:abc",
    )
    with pytest.raises(OSError):
        write_run_env(env, tmp_path / "no" / "such" / "run-env.yaml")


def test_write_to_directory_path_raises_oserror(tmp_path):
    env = RunEnv(
        python_bin="/v/bin/python",
        venv_dir="/v",
        requirements_digest="sha256:abc",
    )
    with pytest.raises(OSError):
        write_run_env(env, tmp_path)


# --- frozen ---


def test_runenv_is_frozen():
    env = RunEnv(
        python_bin="/v/bin/python",
        venv_dir="/v",
        requirements_digest="sha256:abc",
    )
    with pytest.raises(Exception):
        env.python_bin = "x"  # type: ignore[misc]
