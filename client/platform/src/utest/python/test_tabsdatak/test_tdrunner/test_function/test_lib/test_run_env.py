#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tabsdatak._tdrunner.function._lib.run_env import (
    InvalidRunEnvError,
    RunEnv,
    load_run_env,
    write_run_env,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def _write(tmp_path: Path, body: str, name: str = "run-env.yaml") -> Path:
    p = tmp_path / name
    p.write_text(body)
    return p


# --- happy path / roundtrip ---


def test_default_extra_env_is_empty():
    env = RunEnv(
        python_bin="/v/bin/python",
        venv_dir="/v",
        requirements_digest="sha256:abc",
    )
    assert env.extra_env == {}


def test_roundtrip_minimal(tmp_path):
    env = RunEnv(
        python_bin="/v/bin/python",
        venv_dir="/v",
        requirements_digest="sha256:abc",
    )
    out = tmp_path / "run-env.yaml"
    write_run_env(env, out)
    assert load_run_env(out) == env


def test_roundtrip_explicit_empty_extra_env(tmp_path):
    env = RunEnv(
        python_bin="/v/bin/python",
        venv_dir="/v",
        requirements_digest="sha256:abc",
        extra_env={},
    )
    out = tmp_path / "run-env.yaml"
    write_run_env(env, out)
    assert load_run_env(out) == env


def test_roundtrip_with_extra_env(tmp_path):
    env = RunEnv(
        python_bin="/v/bin/python",
        venv_dir="/v",
        requirements_digest="sha256:abc",
        extra_env={"PYTHONPATH": "/foo:/bar", "TZ": "UTC"},
    )
    out = tmp_path / "run-env.yaml"
    write_run_env(env, out)
    assert load_run_env(out) == env


def test_load_extra_env_omitted_defaults_to_empty(tmp_path):
    p = _write(
        tmp_path,
        "python_bin: /v/p\nvenv_dir: /v\nrequirements_digest: sha256:a\n",
    )
    env = load_run_env(p)
    assert env.extra_env == {}


def test_load_extra_env_explicit_null_defaults_to_empty(tmp_path):
    p = _write(
        tmp_path,
        "python_bin: /v/p\nvenv_dir: /v\nrequirements_digest: sha256:a\n"
        "extra_env: null\n",
    )
    env = load_run_env(p)
    assert env.extra_env == {}


# --- file/yaml errors ---


def test_load_missing_file_raises_filenotfound(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_run_env(tmp_path / "nope.yaml")


def test_load_malformed_yaml(tmp_path):
    p = _write(tmp_path, "python_bin: { unclosed")
    with pytest.raises(InvalidRunEnvError, match="malformed YAML"):
        load_run_env(p)


def test_load_top_level_not_a_mapping(tmp_path):
    p = _write(tmp_path, "- a\n- b\n")
    with pytest.raises(InvalidRunEnvError, match="must be a mapping"):
        load_run_env(p)


# --- missing/wrong required fields ---


@pytest.mark.parametrize("missing", ["python_bin", "venv_dir", "requirements_digest"])
def test_load_missing_required_field(tmp_path, missing):
    fields = {
        "python_bin": "/v/bin/python",
        "venv_dir": "/v",
        "requirements_digest": "sha256:abc",
    }
    fields.pop(missing)
    p = _write(tmp_path, yaml.safe_dump(fields))
    with pytest.raises(InvalidRunEnvError, match=missing):
        load_run_env(p)


def test_load_python_bin_wrong_type(tmp_path):
    p = _write(
        tmp_path,
        "python_bin: 42\nvenv_dir: /v\nrequirements_digest: sha256:a\n",
    )
    with pytest.raises(InvalidRunEnvError, match="python_bin"):
        load_run_env(p)


def test_load_venv_dir_wrong_type(tmp_path):
    p = _write(
        tmp_path,
        "python_bin: /v/p\nvenv_dir: 42\nrequirements_digest: sha256:a\n",
    )
    with pytest.raises(InvalidRunEnvError, match="venv_dir"):
        load_run_env(p)


def test_load_requirements_digest_wrong_type(tmp_path):
    p = _write(
        tmp_path,
        "python_bin: /v/p\nvenv_dir: /v\nrequirements_digest: 42\n",
    )
    with pytest.raises(InvalidRunEnvError, match="requirements_digest"):
        load_run_env(p)


def test_load_requirements_digest_missing_prefix(tmp_path):
    p = _write(
        tmp_path,
        "python_bin: /v/p\nvenv_dir: /v\nrequirements_digest: abc\n",
    )
    with pytest.raises(InvalidRunEnvError, match="sha256:"):
        load_run_env(p)


# --- extra_env errors ---


def test_load_extra_env_not_a_mapping(tmp_path):
    p = _write(
        tmp_path,
        "python_bin: /v/p\nvenv_dir: /v\nrequirements_digest: sha256:a\n"
        "extra_env: not-a-map\n",
    )
    with pytest.raises(InvalidRunEnvError, match="extra_env"):
        load_run_env(p)


def test_load_extra_env_key_not_string(tmp_path):
    p = _write(
        tmp_path,
        "python_bin: /v/p\nvenv_dir: /v\nrequirements_digest: sha256:a\n"
        "extra_env:\n  42: ok\n",
    )
    with pytest.raises(InvalidRunEnvError, match="extra_env"):
        load_run_env(p)


def test_load_extra_env_value_not_string(tmp_path):
    p = _write(
        tmp_path,
        "python_bin: /v/p\nvenv_dir: /v\nrequirements_digest: sha256:a\n"
        "extra_env:\n  KEY: 42\n",
    )
    with pytest.raises(InvalidRunEnvError, match="extra_env"):
        load_run_env(p)


# --- write errors ---


def test_write_to_missing_parent_raises_oserror(tmp_path):
    env = RunEnv("/v/p", "/v", "sha256:a")
    with pytest.raises(OSError):
        write_run_env(env, tmp_path / "no" / "such" / "run-env.yaml")


def test_write_to_directory_path_raises_oserror(tmp_path):
    env = RunEnv("/v/p", "/v", "sha256:a")
    with pytest.raises(OSError):
        write_run_env(env, tmp_path)


# --- frozen ---


def test_runenv_is_frozen():
    env = RunEnv("/v/p", "/v", "sha256:a")
    with pytest.raises(Exception):
        env.python_bin = "x"  # type: ignore[misc]
