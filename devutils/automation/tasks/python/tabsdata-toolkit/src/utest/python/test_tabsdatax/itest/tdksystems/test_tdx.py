"""Tests for the tdx-system / tdx-domain command layer (tabsdatax.itest.tdksystems.tdx).

We exercise the fs backend end-to-end (no Docker / creds needed) plus the
type lookup, argument shape, role validation and info-format handling. Systems
and domains are addressed by name; the `stores` fixture points the stores at
tmp dirs and files are located with `store.system_path` / `store.domain_path`
(the same resolution the CLI uses). Data loading (`--model` / `--data`) is
covered by the integration suite."""
from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import pytest

from tabsdatax.itest.tdksystems import descriptor, store, tdx
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
    assert len(systems.all_slugs()) == 17
    for slug in systems.all_slugs():
        assert systems.get(slug).slug == slug


def test_unknown_type_rejected():
    with pytest.raises(DatagenError, match="unknown --type"):
        tdx._sys_create(["--type", "nope", "--sys", "n"])


# ---- $TD_BACKENDS_CRYPTO_KEY gate ---------------------------------------------------

def test_dispatch_requires_secret(monkeypatch):
    # Every real verb is gated on the secret (checked before it runs).
    monkeypatch.delenv("TD_BACKENDS_CRYPTO_KEY", raising=False)
    with pytest.raises(DatagenError, match="TD_BACKENDS_CRYPTO_KEY must be set"):
        tdx._dispatch("tdx-system", tdx._SYS_VERBS, ["status", "--sys", "x"])
    with pytest.raises(DatagenError, match="TD_BACKENDS_CRYPTO_KEY must be set"):
        tdx._dispatch("tdx-domain", tdx._DOMAIN_VERBS,
                      ["create", "--sys", "s", "--domain", "d"])


def test_short_secret_rejected(monkeypatch, stores, fs_root):
    monkeypatch.setenv("TD_BACKENDS_CRYPTO_KEY", "tooshort")   # < 12
    with pytest.raises(DatagenError, match="at least 12 characters"):
        tdx._dispatch("tdx-system", tdx._SYS_VERBS,
                      ["create", "--type", "fs", "--sys", "my_fs",
                       "--sys-path", str(fs_root), "-f"])


# ---- fs system lifecycle: create -> info -> status -> destroy --------------

def _read(path):
    # descriptors are encrypted on disk; load() decrypts + parses.
    return descriptor.load(path)


def test_sys_create_writes_stamped_descriptor(stores, fs_root):
    tdx._sys_create(["--type", "fs", "--sys", "my_fs",
                     "--sys-path", str(fs_root), "-f"])
    doc = _read(store.system_path("my_fs"))
    assert doc["category"] == "cloud" and doc["system"] == "fs"
    assert doc["role"] == "sys"
    assert doc["name"] == "my_fs"
    assert "prefix" not in doc            # info keys are unprefixed now
    assert doc["started"]                 # stamped
    assert fs_root.is_dir()


def test_sys_create_fails_if_descriptor_exists(stores, fs_root):
    args = ["--type", "fs", "--sys", "my_fs", "--sys-path", str(fs_root)]
    assert tdx._sys_create(args + ["-f"]) == 0
    # Re-create without --force: refuse to clobber the existing descriptor.
    with pytest.raises(DatagenError, match="descriptor file already exists"):
        tdx._sys_create(args)
    # --force overwrites.
    assert tdx._sys_create(args + ["-f"]) == 0


def test_domain_create_fails_if_descriptor_exists(stores, fs_root):
    tdx._sys_create(["--type", "fs", "--sys", "my_fs",
                     "--sys-path", str(fs_root), "-f"])
    dom = ["--sys", "my_fs", "--domain", "inbox"]
    assert tdx._domain_create(dom + ["-f"]) == 0
    with pytest.raises(DatagenError, match="descriptor file already exists"):
        tdx._domain_create(dom)


def test_created_info_hint_uses_verb_first_syntax(stores, fs_root, capsys):
    tdx._sys_create(["--type", "fs", "--sys", "my_fs",
                     "--sys-path", str(fs_root), "-f"])
    # The post-create hint must be a runnable command: verb before flags,
    # addressing the system by name.
    assert "tdx-system info --sys my_fs --format json" in capsys.readouterr().out
    tdx._domain_create(["--sys", "my_fs", "--domain", "inbox", "-f"])
    assert "tdx-domain info --domain inbox --format json" in capsys.readouterr().out


def test_sys_info_formats_are_unprefixed(stores, fs_root):
    tdx._sys_create(["--type", "fs", "--sys", "my_fs",
                     "--sys-path", str(fs_root), "-f"])
    doc = descriptor.load(store.system_path("my_fs"))
    obj = json.loads(descriptor.info(doc, "json"))
    assert obj["NAME"] == "my_fs"
    assert obj["TYPE"] == "fs"
    assert obj["SYS_PATH"] == str(fs_root)
    exp = descriptor.info(doc, "export")
    assert "export NAME=my_fs" in exp
    assert descriptor.info(doc, "unset").splitlines()[0] == "unset NAME"


def test_name_must_be_identifier(stores, fs_root):
    with pytest.raises(DatagenError, match="must be an identifier"):
        tdx._sys_create(["--type", "fs", "--sys", "not-an-ident",
                         "--sys-path", str(fs_root), "-f"])


def test_full_fs_lifecycle(stores, fs_root):
    tdx._sys_create(["--type", "fs", "--sys", "my_fs",
                     "--sys-path", str(fs_root), "-f"])
    assert tdx._sys_status(["--sys", "my_fs"]) == 0

    tdx._domain_create(["--sys", "my_fs", "--domain", "inbox", "-f"])
    dom = _read(store.domain_path("inbox"))
    assert dom["role"] == "env" and dom["name"] == "inbox"
    assert (fs_root / "inbox").is_dir()
    dinfo = json.loads(descriptor.info(descriptor.load(store.domain_path("inbox")),
                                       "json"))
    assert dinfo["BASE_PATH"] == f"{fs_root}/inbox"
    assert tdx._domain_status(["--domain", "inbox"]) == 0

    tdx._domain_destroy(["--domain", "inbox"])
    assert not store.domain_path("inbox").exists()
    assert not (fs_root / "inbox").exists()

    tdx._sys_destroy(["--sys", "my_fs"])
    assert not store.system_path("my_fs").exists()
    assert not fs_root.exists()


# ---- list: names + types in the store dir ----------------------------------

def test_sys_list_and_domain_list(stores, fs_root, capsys):
    tdx._sys_create(["--type", "fs", "--sys", "alpha",
                     "--sys-path", str(fs_root / "a"), "-f"])
    tdx._sys_create(["--type", "fs", "--sys", "beta",
                     "--sys-path", str(fs_root / "b"), "-f"])
    tdx._domain_create(["--sys", "alpha", "--domain", "inbox", "-f"])
    capsys.readouterr()   # drop the create output

    assert tdx._sys_list([]) == 0
    out = capsys.readouterr().out
    assert "alpha" in out and "beta" in out and "fs" in out

    assert tdx._domain_list([]) == 0
    out = capsys.readouterr().out
    assert "inbox" in out and "fs" in out


def test_domain_list_shows_model_and_data(stores, fs_root, capsys):
    tdx._sys_create(["--type", "fs", "--sys", "alpha",
                     "--sys-path", str(fs_root), "-f"])
    tdx._domain_create(["--sys", "alpha", "--domain", "inbox", "-f"])
    # Stamp model/data directly (no data_gen run) to check the list rendering.
    descriptor.stamp(store.domain_path("inbox"), name="inbox", started="t",
                     model="world", data="xs")
    capsys.readouterr()
    tdx._domain_list([])
    out = capsys.readouterr().out
    assert "inbox" in out and "model=world" in out and "data=xs" in out


def test_list_empty_store_is_friendly(stores, capsys):
    tdx._sys_list([])
    assert "no systems in" in capsys.readouterr().out


# ---- --secret-env: which env var holds the personal secret ------------------

def test_secret_env_selects_env_var(stores, fs_root, monkeypatch):
    # Create under a custom secret env var (the default var is irrelevant here).
    monkeypatch.setenv("MY_SECRET", "custom-secret-abcdef")
    tdx._sys_create(["--type", "fs", "--sys", "cs", "--sys-path", str(fs_root),
                     "--secret-env", "MY_SECRET", "-f"])
    # Reading it back under MY_SECRET works...
    assert tdx._sys_status(["--sys", "cs", "--secret-env", "MY_SECRET"]) == 0
    # ...but the default secret is the wrong key -> can't decrypt.
    with pytest.raises(DatagenError):
        tdx._sys_status(["--sys", "cs"])


def test_secret_env_unset_fails(stores):
    with pytest.raises(DatagenError, match="must be set and at least 12"):
        tdx._sys_list(["--secret-env", "DEFINITELY_UNSET_SECRET"])


# ---- role validation + info format guard -----------------------------------

def test_sys_verb_rejects_domain_descriptor(stores, fs_root):
    tdx._sys_create(["--type", "fs", "--sys", "my_fs",
                     "--sys-path", str(fs_root), "-f"])
    tdx._domain_create(["--sys", "my_fs", "--domain", "inbox", "-f"])
    # Cross-wire the store dirs so a system verb reads the domain file (and
    # vice versa): the role guard must reject the wrong-role descriptor.
    with pytest.raises(DatagenError, match="use `tdx-domain`, not `tdx-system`"):
        tdx._sys_status(["--sys", "inbox", "--sys-dir", str(store.domain_dir())])
    with pytest.raises(DatagenError, match="use `tdx-system`, not `tdx-domain`"):
        tdx._domain_status(["--domain", "my_fs",
                            "--domain-dir", str(store.system_dir())])


def test_domain_data_requires_model(stores, fs_root):
    tdx._sys_create(["--type", "fs", "--sys", "my_fs",
                     "--sys-path", str(fs_root), "-f"])
    with pytest.raises(DatagenError, match="--data requires --model"):
        tdx._domain_create(["--sys", "my_fs", "--domain", "inbox",
                            "--data", "xs", "-f"])


def test_info_bad_format_rejected(stores, fs_root):
    tdx._sys_create(["--type", "fs", "--sys", "my_fs",
                     "--sys-path", str(fs_root), "-f"])
    # argparse rejects an out-of-choice --format with SystemExit.
    with pytest.raises(SystemExit):
        tdx._sys_info(["--sys", "my_fs", "--format", "yaml"])


def test_pause_resume_registered_and_container_only(stores, fs_root):
    # pause / resume are tdx-system verbs...
    assert {"pause", "resume"} <= set(tdx._SYS_VERBS)
    # ...but only apply to container systems (fs has no process to pause).
    tdx._sys_create(["--type", "fs", "--sys", "my_fs",
                     "--sys-path", str(fs_root), "-f"])
    with pytest.raises(DatagenError, match="pause only applies to container"):
        tdx._sys_pause(["--sys", "my_fs"])
    with pytest.raises(DatagenError, match="resume only applies to container"):
        tdx._sys_resume(["--sys", "my_fs"])
