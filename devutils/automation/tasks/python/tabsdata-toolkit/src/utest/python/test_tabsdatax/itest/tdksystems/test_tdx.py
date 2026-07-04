"""Tests for the tdx-system / tdx-domain command layer (tabsdatax.itest.tdksystems.tdx).

We exercise the fs backend end-to-end (no Docker / creds needed) plus the
type lookup, argument shape, role validation and info-format handling. Data
loading (`--model` / `--data`) is covered by the integration suite."""
from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import pytest

from tabsdatax.itest.tdksystems import descriptor, tdx
from tabsdatax.itest.tdksystems.errors import DatagenError


@pytest.fixture
def fs_root(tmp_path):
    """fs validates that an absolute root lives under /tmp|/home|/Users, so a
    pytest tmp_path (under /private/var on macOS) won't do."""
    p = Path(f"/tmp/tdx-utest-{os.getpid()}-{tmp_path.name}")
    yield p
    shutil.rmtree(p, ignore_errors=True)


# ---- registry covers every bundled system ---------------------------------

def test_registry_covers_every_system():
    from tabsdatax.itest.tdksystems import systems
    assert len(systems.all_slugs()) == 16
    for slug in systems.all_slugs():
        assert systems.get(slug).slug == slug


def test_unknown_type_rejected():
    with pytest.raises(DatagenError, match="unknown --type"):
        tdx._sys_create(["--type", "nope", "--sys", "/tmp/x.json", "--name", "n"])


# ---- $TD_DEV_PERSONAL_SECRET gate ---------------------------------------------------

def test_dispatch_requires_secret(monkeypatch):
    # Every real verb is gated on the secret (checked before it runs).
    monkeypatch.delenv("TD_DEV_PERSONAL_SECRET", raising=False)
    with pytest.raises(DatagenError, match="TD_DEV_PERSONAL_SECRET must be set"):
        tdx._dispatch("tdx-system", tdx._SYS_VERBS, ["status", "--sys", "x.json"])
    with pytest.raises(DatagenError, match="TD_DEV_PERSONAL_SECRET must be set"):
        tdx._dispatch("tdx-domain", tdx._DOMAIN_VERBS,
                      ["create", "--sys", "s.json", "--domain", "d.json",
                       "--name", "n"])


def test_short_secret_rejected(monkeypatch, tmp_path, fs_root):
    monkeypatch.setenv("TD_DEV_PERSONAL_SECRET", "tooshort")   # < 12
    with pytest.raises(DatagenError, match="at least 12 characters"):
        tdx._dispatch("tdx-system", tdx._SYS_VERBS,
                      ["create", "--type", "fs", "--sys", str(tmp_path / "fs.json"),
                       "--name", "my_fs", "--sys-path", str(fs_root), "-f"])


# ---- fs system lifecycle: create -> info -> status -> destroy --------------

def _read(path):
    # descriptors are encrypted on disk; load() decrypts + parses.
    return descriptor.load(path)


def test_sys_create_writes_stamped_descriptor(tmp_path, fs_root):
    root = fs_root
    sysd = tmp_path / "fs.json"
    tdx._sys_create(["--type", "fs", "--sys", str(sysd),
                     "--name", "my_fs", "--sys-path", str(root), "-f"])
    doc = _read(sysd)
    assert doc["category"] == "cloud" and doc["system"] == "fs"
    assert doc["role"] == "sys"
    assert doc["name"] == "my_fs"
    assert doc["prefix"] == "MY_FS"
    assert doc["started"]           # stamped
    assert root.is_dir()


def test_sys_create_fails_if_descriptor_exists(tmp_path, fs_root):
    sysd = tmp_path / "fs.json"
    args = ["--type", "fs", "--sys", str(sysd), "--name", "my_fs",
            "--sys-path", str(fs_root)]
    assert tdx._sys_create(args + ["-f"]) == 0
    # Re-create without --force: refuse to clobber the existing descriptor.
    with pytest.raises(DatagenError, match="descriptor file already exists"):
        tdx._sys_create(args)
    # --force overwrites.
    assert tdx._sys_create(args + ["-f"]) == 0


def test_domain_create_fails_if_descriptor_exists(tmp_path, fs_root):
    sysd = tmp_path / "fs.json"
    domd = tmp_path / "d.json"
    tdx._sys_create(["--type", "fs", "--sys", str(sysd), "--name", "my_fs",
                     "--sys-path", str(fs_root), "-f"])
    dom = ["--sys", str(sysd), "--domain", str(domd), "--name", "inbox"]
    assert tdx._domain_create(dom + ["-f"]) == 0
    with pytest.raises(DatagenError, match="descriptor file already exists"):
        tdx._domain_create(dom)


def test_created_info_hint_uses_verb_first_syntax(tmp_path, fs_root, capsys):
    sysd = tmp_path / "fs.json"
    domd = tmp_path / "d.json"
    tdx._sys_create(["--type", "fs", "--sys", str(sysd), "--name", "my_fs",
                     "--sys-path", str(fs_root), "-f"])
    # The post-create hint must be a runnable command: verb before flags.
    assert f"tdx-system info --sys {sysd} --format json" in capsys.readouterr().out
    tdx._domain_create(["--sys", str(sysd), "--domain", str(domd),
                        "--name", "inbox", "-f"])
    assert f"tdx-domain info --domain {domd} --format json" in capsys.readouterr().out


def test_sys_info_formats_prefixed_by_name(tmp_path, fs_root):
    root = fs_root
    sysd = tmp_path / "fs.json"
    tdx._sys_create(["--type", "fs", "--sys", str(sysd),
                     "--name", "my_fs", "--sys-path", str(root), "-f"])
    doc = descriptor.load(sysd)
    obj = json.loads(descriptor.info(doc, "json"))
    assert obj["MY_FS__NAME"] == "my_fs"
    assert obj["MY_FS__TYPE"] == "fs"
    assert obj["MY_FS__SYS_PATH"] == str(root)
    exp = descriptor.info(doc, "export")
    assert "export MY_FS__NAME=my_fs" in exp
    assert descriptor.info(doc, "unset").splitlines()[0] == "unset MY_FS__NAME"


def test_name_must_be_identifier(tmp_path, fs_root):
    with pytest.raises(DatagenError, match="must be an identifier"):
        tdx._sys_create(["--type", "fs", "--sys", str(tmp_path / "x.json"),
                         "--name", "not-an-ident", "--sys-path",
                         str(fs_root), "-f"])


def test_full_fs_lifecycle(tmp_path, fs_root):
    root = fs_root
    sysd = tmp_path / "fs.json"
    domd = tmp_path / "d.json"
    tdx._sys_create(["--type", "fs", "--sys", str(sysd),
                     "--name", "my_fs", "--sys-path", str(root), "-f"])
    assert tdx._sys_status(["--sys", str(sysd)]) == 0

    tdx._domain_create(["--sys", str(sysd), "--domain", str(domd),
                        "--name", "inbox", "-f"])
    dom = _read(domd)
    assert dom["role"] == "env" and dom["name"] == "inbox"
    assert (root / "inbox").is_dir()
    dinfo = json.loads(descriptor.info(descriptor.load(domd), "json"))
    assert dinfo["INBOX__BASE_PATH"] == f"{root}/inbox"
    assert tdx._domain_status(["--domain", str(domd)]) == 0

    tdx._domain_destroy(["--domain", str(domd)])
    assert not domd.exists()
    assert not (root / "inbox").exists()

    tdx._sys_destroy(["--sys", str(sysd)])
    assert not sysd.exists()
    assert not root.exists()


# ---- role validation + info format guard -----------------------------------

def test_sys_verb_rejects_domain_descriptor(tmp_path, fs_root):
    root = fs_root
    sysd = tmp_path / "fs.json"
    domd = tmp_path / "d.json"
    tdx._sys_create(["--type", "fs", "--sys", str(sysd),
                     "--name", "my_fs", "--sys-path", str(root), "-f"])
    tdx._domain_create(["--sys", str(sysd), "--domain", str(domd),
                        "--name", "inbox", "-f"])
    with pytest.raises(DatagenError, match="use `tdx-domain`, not `tdx-system`"):
        tdx._sys_status(["--sys", str(domd)])
    with pytest.raises(DatagenError, match="use `tdx-system`, not `tdx-domain`"):
        tdx._domain_status(["--domain", str(sysd)])


def test_domain_data_requires_model(tmp_path, fs_root):
    root = fs_root
    sysd = tmp_path / "fs.json"
    tdx._sys_create(["--type", "fs", "--sys", str(sysd),
                     "--name", "my_fs", "--sys-path", str(root), "-f"])
    with pytest.raises(DatagenError, match="--data requires --model"):
        tdx._domain_create(["--sys", str(sysd), "--domain", str(tmp_path / "d.json"),
                            "--name", "inbox", "--data", "xs", "-f"])


def test_info_bad_format_rejected(tmp_path, fs_root):
    root = fs_root
    sysd = tmp_path / "fs.json"
    tdx._sys_create(["--type", "fs", "--sys", str(sysd),
                     "--name", "my_fs", "--sys-path", str(root), "-f"])
    # argparse rejects an out-of-choice --format with SystemExit.
    with pytest.raises(SystemExit):
        tdx._sys_info(["--sys", str(sysd), "--format", "yaml"])


def test_pause_resume_registered_and_container_only(tmp_path, fs_root):
    # pause / resume are tdx-system verbs...
    assert {"pause", "resume"} <= set(tdx._SYS_VERBS)
    # ...but only apply to container systems (fs has no process to pause).
    sysd = tmp_path / "fs.json"
    tdx._sys_create(["--type", "fs", "--sys", str(sysd),
                     "--name", "my_fs", "--sys-path", str(fs_root), "-f"])
    with pytest.raises(DatagenError, match="pause only applies to container"):
        tdx._sys_pause(["--sys", str(sysd)])
    with pytest.raises(DatagenError, match="resume only applies to container"):
        tdx._sys_resume(["--sys", str(sysd)])
