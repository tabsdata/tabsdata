"""Tests for the tdx-system / tdx-domain command layer (tabsdatax.itest.tdksystems.tdx).

We exercise the fs backend end-to-end (no Docker / creds needed) plus the
type lookup, argument shape, role validation and info-format handling. Systems
and domains are addressed by name; the `stores` fixture points the stores at
tmp dirs and files are located with `store.system_path` / `store.domain_path`
(the same resolution the CLI uses). Data loading (`--model` / `--data`) is
covered by the integration suite.

By default a domain's external entity is named `<DOMAIN_PREFIX><domain>` (see
`_resolve_domain_prefix`), which depends on the host user; tests that assert the
concrete fs path / external name pin `--domain-prefix ""` so the entity name is
just the domain name. The prefixing itself is covered by its own tests below."""
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
    assert doc["category"] == "native" and doc["system"] == "fs"
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


def test_install_model_and_data_loads_without_rerunning_ddl(monkeypatch):
    # create --model --data installs the schema once (data create) then loads with
    # force=False — the DDL must NOT run twice (db2/oracle reject the second
    # CREATE TABLE, since their force-load table-drop can't see the domain schema).
    seen = {}
    monkeypatch.setattr(tdx.cli, "_run_data_create",
                        lambda ns: seen.__setitem__("create_force", ns.force))
    monkeypatch.setattr(tdx.cli, "_run_data_load",
                        lambda ns: seen.__setitem__("load_force", ns.force))
    monkeypatch.setattr(tdx.data_gen, "is_sizeable", lambda m: True)
    tdx._install_model_and_data("/tmp/x/dom", "world", "xs", force=True)
    assert seen == {"create_force": True, "load_force": False}


def test_domain_create_force_destroys_existing_via_system(stores, fs_root, monkeypatch):
    # create -f on an existing domain destroys it FIRST (over the system's
    # connection, via _domain_destroy_one), then creates fresh.
    tdx._sys_create(["--type", "fs", "--sys", "a", "--sys-path", str(fs_root), "-f"])
    tdx._domain_create(["--sys", "a", "--domain", "d", "--domain-prefix", "", "-f"])
    calls = []
    orig = tdx._domain_destroy_one
    monkeypatch.setattr(
        tdx, "_domain_destroy_one",
        lambda name, dd, sd: calls.append((name, sd)) or orig(name, dd, sd),
    )
    assert tdx._domain_create(
        ["--sys", "a", "--domain", "d", "--domain-prefix", "", "-f"]
    ) == 0
    assert calls == [("d", None)]           # destroyed via the system before recreate
    assert (fs_root / "d").is_dir()         # and recreated
    # a fresh create (no existing descriptor) does NOT destroy first.
    calls.clear()
    tdx._domain_create(["--sys", "a", "--domain", "e", "--domain-prefix", "", "-f"])
    assert calls == []


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

    tdx._domain_create(["--sys", "my_fs", "--domain", "inbox",
                        "--domain-prefix", "", "-f"])
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
    assert fs_root.exists()  # system destroy leaves the root path in place


def test_fs_create_ignores_pre_existing_root(stores, fs_root):
    # A pre-existing (even non-empty) root is fine: create neither errors nor
    # clears it — no --force needed — and system destroy leaves it untouched.
    fs_root.mkdir(parents=True)
    (fs_root / "keep.txt").write_text("x")
    assert tdx._sys_create(["--type", "fs", "--sys", "my_fs",
                            "--sys-path", str(fs_root)]) == 0
    assert (fs_root / "keep.txt").read_text() == "x"
    tdx._sys_destroy(["--sys", "my_fs"])
    assert (fs_root / "keep.txt").exists()


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
    assert "System" in out and "Type" in out         # column titles

    assert tdx._domain_list([]) == 0
    out = capsys.readouterr().out
    assert "inbox" in out and "fs" in out
    assert "Domain" in out and "System" in out        # column titles


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


def test_list_wrong_key_fails_loudly(stores, fs_root, monkeypatch):
    # A wrong key must not make `list` silently report nothing: it fails saying
    # the key is incorrect.
    tdx._sys_create(["--type", "fs", "--sys", "alpha",
                     "--sys-path", str(fs_root), "-f"])
    tdx._domain_create(["--sys", "alpha", "--domain", "inbox", "-f"])
    monkeypatch.setenv("TD_BACKENDS_CRYPTO_KEY", "a-different-good-secret")
    with pytest.raises(DatagenError, match="could not read descriptor"):
        tdx._sys_list([])
    with pytest.raises(DatagenError, match="could not read descriptor"):
        tdx._domain_list([])


def test_list_fails_loudly_on_stale_descriptor_label(stores):
    # A descriptor written by an older tdx (a different PEM label) must NOT be
    # silently skipped as a sidecar: list surfaces it as a read error naming the
    # label mismatch. The body is irrelevant — the label is rejected first.
    sysdir = store.system_dir()
    sysdir.mkdir(parents=True, exist_ok=True)
    (sysdir / "legacy").write_text(
        "-----BEGIN TD_GEN DESCRIPTOR-----\nAAAA\n-----END TD_GEN DESCRIPTOR-----\n"
    )
    with pytest.raises(DatagenError, match="older or incompatible tdx"):
        tdx._sys_list([])


def test_list_skips_non_descriptor_sidecars(stores, fs_root, capsys):
    # A stray non-descriptor file in the store dir is skipped, not mistaken for a
    # bad-key descriptor (only our PEM-armored files can trigger the key error).
    tdx._sys_create(["--type", "fs", "--sys", "alpha",
                     "--sys-path", str(fs_root), "-f"])
    (store.system_dir() / "alpha.ssh.log").write_text("not a descriptor\n")
    capsys.readouterr()
    tdx._sys_list([])
    out = capsys.readouterr().out
    assert "alpha" in out and "alpha.ssh.log" not in out


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


# ---- tdx-system types: list every registered system type -------------------

def test_types_lists_all_systems(capsys):
    from tabsdatax.itest.tdksystems import systems
    # `types` is descriptor-free (no secret / venv needed) and just reads the
    # registry.
    assert "types" in tdx._SYS_VERBS and "types" in tdx._NO_DESCRIPTOR_VERBS
    assert tdx._sys_types([]) == 0
    out = capsys.readouterr().out
    for slug in systems.all_slugs():
        assert slug in out
    assert f"{len(systems.all_slugs())} types" in out
    # container systems show their platform then image (repo:tag); others don't.
    mysql = systems.get("mysql")
    assert f"{mysql.image_platform}  {mysql.image}" in out
    assert "container" in out and "cloud" in out and "native" in out  # fs -> native


# ---- tdx-system fetch-images: docker pull every container image ------------

def test_fetch_images_pulls_every_container_image(monkeypatch, capsys):
    from tabsdatax.itest.tdksystems import systems
    # fetch-images is descriptor-free (no secret / venv) — registry + docker pull.
    assert "fetch-images" in tdx._SYS_VERBS
    assert "fetch-images" in tdx._NO_DESCRIPTOR_VERBS
    monkeypatch.setattr(tdx, "require_docker", lambda: None)
    pulled: list[tuple[str, str]] = []
    monkeypatch.setattr(tdx, "docker_pull",
                        lambda image, platform="": pulled.append((image, platform)))
    assert tdx._sys_fetch_images([]) == 0
    # Exactly the container systems' (image, platform) pairs were pulled — no
    # cloud / saas / native system (which have no image) was.
    container = [systems.get(s) for s in systems.all_slugs()
                 if systems.get(s).category == "container"]
    expected = {(c.image, c.image_platform) for c in container}
    assert set(pulled) == expected
    out = capsys.readouterr().out
    assert f"fetched {len(expected)}/{len(expected)} image(s)" in out


def test_fetch_images_reports_failures(monkeypatch, capsys):
    monkeypatch.setattr(tdx, "require_docker", lambda: None)

    def boom(image, platform=""):
        raise DatagenError("no such image")

    monkeypatch.setattr(tdx, "docker_pull", boom)
    assert tdx._sys_fetch_images([]) == 1
    out = capsys.readouterr().out
    assert "failed to pull" in out and "failed" in out


# ---- create --auto: one system per type, named after the type --------------

def test_sys_create_auto_fs(stores, tmp_path, monkeypatch, capsys):
    # fs takes no env: --auto names the system after its type and defaults its
    # root to target/backends/<system name> under the current directory (tmp cwd).
    monkeypatch.chdir(tmp_path)
    assert tdx._sys_create(["--auto", "fs", "-f"]) == 0
    doc = _read(store.system_path("fs"))
    assert doc["system"] == "fs" and doc["name"] == "fs"    # named after its type
    assert (tmp_path / "target" / "backends" / "fs").is_dir()
    assert "created 1/1 systems" in capsys.readouterr().out


def test_sys_create_auto_continues_on_failure(stores, tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert tdx._sys_create(["--auto", "fs", "-f"]) == 0
    capsys.readouterr()
    # Re-run without --force: fs already exists -> that item FAILs, but the
    # batch keeps going and the non-zero rc reflects the failure.
    assert tdx._sys_create(["--auto", "fs"]) == 1
    out = capsys.readouterr().out
    assert "FAIL" in out and "fs" in out and "already exists" in out


def test_sys_create_auto_unknown_type_rejected(stores):
    with pytest.raises(DatagenError, match="unknown type"):
        tdx._sys_create(["--auto", "fs,nope"])


def test_create_help_advertises_auto(capsys):
    # `create -h` routes to single mode, so it must still document the batch
    # --auto mode — otherwise it's undiscoverable (you'd have to already know to
    # type `create --auto -h`).
    for verb in (tdx._sys_create, tdx._domain_create):
        with pytest.raises(SystemExit):
            verb(["-h"])
        assert "--auto" in capsys.readouterr().out


def test_all_is_reserved_as_a_system_name(stores, fs_root):
    with pytest.raises(DatagenError, match="reserved"):
        tdx._sys_create(["--type", "fs", "--sys", "ALL",
                         "--sys-path", str(fs_root), "-f"])


# ---- status / destroy ALL + comma over many systems -----------------------

def test_sys_status_and_destroy_all(stores, fs_root, capsys):
    tdx._sys_create(["--type", "fs", "--sys", "alpha",
                     "--sys-path", str(fs_root / "a"), "-f"])
    tdx._sys_create(["--type", "fs", "--sys", "beta",
                     "--sys-path", str(fs_root / "b"), "-f"])
    capsys.readouterr()

    assert tdx._sys_status(["--auto", "ALL"]) == 0
    out = capsys.readouterr().out
    assert "alpha" in out and "beta" in out and "2/2 systems ready" in out

    # A comma list selects a subset.
    assert tdx._sys_status(["--auto", "alpha,beta"]) == 0
    capsys.readouterr()

    assert tdx._sys_destroy(["--auto", "ALL"]) == 0
    assert not store.system_path("alpha").exists()
    assert not store.system_path("beta").exists()
    assert "destroyed 2/2 systems" in capsys.readouterr().out


# ---- tdx-domain create --auto: one domain per system ----------------------

def test_domain_create_auto_names_after_system(stores, fs_root, capsys):
    # each domain is named after its system (verbatim, no prefix).
    tdx._sys_create(["--type", "fs", "--sys", "alpha",
                     "--sys-path", str(fs_root / "a"), "-f"])
    tdx._sys_create(["--type", "fs", "--sys", "beta",
                     "--sys-path", str(fs_root / "b"), "-f"])
    capsys.readouterr()
    assert tdx._domain_create(["--auto", "ALL", "-f"]) == 0
    assert _read(store.domain_path("alpha"))["name"] == "alpha"
    assert _read(store.domain_path("beta"))["name"] == "beta"
    assert "created 2/2 domains" in capsys.readouterr().out


def test_domain_create_single_rejects_multi_sys(stores, fs_root):
    tdx._sys_create(["--type", "fs", "--sys", "a",
                     "--sys-path", str(fs_root), "-f"])
    with pytest.raises(DatagenError, match="single system"):
        tdx._domain_create(["--sys", "a,b", "--domain", "x", "-f"])


def test_all_is_reserved_as_a_domain_name(stores, fs_root):
    tdx._sys_create(["--type", "fs", "--sys", "my_fs",
                     "--sys-path", str(fs_root), "-f"])
    with pytest.raises(DatagenError, match="reserved"):
        tdx._domain_create(["--sys", "my_fs", "--domain", "ALL", "-f"])


def test_domain_status_and_destroy_auto(stores, fs_root, capsys):
    # Batch status/destroy go through --auto now (ALL or a comma list);
    # --domain is single-target only.
    tdx._sys_create(["--type", "fs", "--sys", "a",
                     "--sys-path", str(fs_root), "-f"])
    tdx._domain_create(["--sys", "a", "--domain", "d1", "-f"])
    tdx._domain_create(["--sys", "a", "--domain", "d2", "-f"])
    capsys.readouterr()
    assert tdx._domain_status(["--auto", "ALL"]) == 0
    assert "2/2 domains ready" in capsys.readouterr().out
    # a comma list selects a subset.
    assert tdx._domain_status(["--auto", "d1,d2"]) == 0
    capsys.readouterr()
    assert tdx._domain_destroy(["--auto", "ALL"]) == 0
    assert not store.domain_path("d1").exists()
    assert not store.domain_path("d2").exists()


def test_domain_destroy_status_domain_is_single_only(stores, fs_root):
    # The old `--domain ALL` / comma form is gone — point users at --auto.
    tdx._sys_create(["--type", "fs", "--sys", "a",
                     "--sys-path", str(fs_root), "-f"])
    tdx._domain_create(["--sys", "a", "--domain", "d1", "-f"])
    with pytest.raises(DatagenError, match="use --auto"):
        tdx._domain_destroy(["--domain", "ALL"])
    with pytest.raises(DatagenError, match="use --auto"):
        tdx._domain_status(["--domain", "d1,d2"])
    # --domain and --auto are mutually exclusive (argparse SystemExit).
    with pytest.raises(SystemExit):
        tdx._domain_destroy(["--domain", "d1", "--auto", "ALL"])


# ---- tdx-domain seed: DDL/data into an existing domain --------------------

def test_seed_registered_and_rejects_multi_domain(stores):
    assert "seed" in tdx._DOMAIN_VERBS
    with pytest.raises(DatagenError, match="single domain"):
        tdx._domain_seed(["--domain", "ALL", "--model", "world"])


def test_seed_rejects_already_seeded_domain(stores, fs_root):
    tdx._sys_create(["--type", "fs", "--sys", "a",
                     "--sys-path", str(fs_root), "-f"])
    tdx._domain_create(["--sys", "a", "--domain", "inbox", "-f"])
    # Mark it seeded (as a real --model create would), then seed must refuse.
    descriptor.stamp(store.domain_path("inbox"), name="inbox", started="t",
                     model="world", data="xs")
    with pytest.raises(DatagenError, match="already seeded"):
        tdx._domain_seed(["--domain", "inbox", "--model", "world"])


# ---- secrets: TD_BACKENDS_SYSTEMS manifest --------------------------------

def test_secrets_emits_systems_var():
    from tabsdatax.itest.tdksystems import secret
    fetched = {"MYSQL__USER": "u", "MYSQL__PASSWORD": "p", "S3__BUCKET": "b"}
    out = secret.with_systems_var(fetched)
    # distinct <ITEM> prefixes, first-seen order; originals preserved.
    assert out[secret.SYSTEMS_ENV] == "MYSQL S3"
    assert out["MYSQL__USER"] == "u"
    # idempotent (the synthetic var doesn't become its own prefix).
    assert secret.with_systems_var(out)[secret.SYSTEMS_ENV] == "MYSQL S3"
    # nothing to list -> no var.
    assert secret.SYSTEMS_ENV not in secret.with_systems_var({})
    # rendered in every --out format.
    assert f"export {secret.SYSTEMS_ENV}='MYSQL S3'" in secret.render(out, "export")
    assert f'"{secret.SYSTEMS_ENV}": "MYSQL S3"' in secret.render(out, "json")
    assert f"unset {secret.SYSTEMS_ENV}" in secret.render(out, "unset")


# ---- tdx-domain recreate: reset to a clean initial state ------------------

def test_recreate_registered():
    assert "recreate" in tdx._DOMAIN_VERBS


def test_system_meta_from_domain_remaps_container_backrefs():
    # container domain: SYS_* back-refs -> the admin fields create_domain reads.
    dmeta = {"HOST": "localhost", "PORT": "3306", "SYS_CONTAINER": "s_mysql",
             "SYS_ADMIN_USER": "root", "SYS_ADMIN_PASSWORD": "pw", "DB": "d"}
    m = tdx._system_meta_from_domain(dmeta)
    assert m["CONTAINER"] == "s_mysql" and m["USER"] == "root" and m["PASSWORD"] == "pw"
    # cloud/fs domain (no SYS_CONTAINER) already carries system settings -> as-is.
    cmeta = {"SYS_PATH": "data", "BUCKET": "b"}
    assert tdx._system_meta_from_domain(cmeta) == cmeta


def test_domain_recreate_empty_wipes_contents(stores, fs_root, capsys):
    tdx._sys_create(["--type", "fs", "--sys", "a",
                     "--sys-path", str(fs_root), "-f"])
    tdx._domain_create(["--sys", "a", "--domain", "inbox",
                        "--domain-prefix", "", "-f"])
    stray = fs_root / "inbox" / "stray.txt"
    stray.write_text("x")
    capsys.readouterr()
    assert tdx._domain_recreate(["--domain", "inbox"]) == 0
    # fs recreate rmtree+mkdir's the domain dir -> back to clean/empty.
    assert (fs_root / "inbox").is_dir()
    assert not stray.exists()
    out = capsys.readouterr().out
    assert "recreated domain 'inbox'" in out and "(empty)" in out


def test_domain_recreate_replays_recorded_model(stores, fs_root):
    tdx._sys_create(["--type", "fs", "--sys", "a",
                     "--sys-path", str(fs_root), "-f"])
    tdx._domain_create(["--sys", "a", "--domain", "inbox",
                        "--domain-prefix", "", "-f"])
    # Record a model (as a --model create would); recreate must preserve it.
    descriptor.stamp(store.domain_path("inbox"), name="inbox", started="t",
                     model="world")
    assert tdx._domain_recreate(["--domain", "inbox"]) == 0
    assert _read(store.domain_path("inbox")).get("model") == "world"
    assert (fs_root / "inbox").is_dir()


def test_domain_recreate_all(stores, fs_root, capsys):
    tdx._sys_create(["--type", "fs", "--sys", "a",
                     "--sys-path", str(fs_root), "-f"])
    tdx._domain_create(["--sys", "a", "--domain", "d1", "--domain-prefix", "", "-f"])
    tdx._domain_create(["--sys", "a", "--domain", "d2", "--domain-prefix", "", "-f"])
    capsys.readouterr()
    assert tdx._domain_recreate(["--domain", "ALL"]) == 0
    assert "recreated 2/2 domains" in capsys.readouterr().out
    assert (fs_root / "d1").is_dir() and (fs_root / "d2").is_dir()


def test_recreate_reuses_credentials_for_sql(stores, monkeypatch):
    # A SQL container resets in place (drop tables) using the domain's own URI,
    # so the name + credentials are reused — no re-provision, no descriptor
    # rewrite. Verify recreate takes that path (not create_domain).
    from tabsdatax.itest.tdksystems.systems._container import ContainerBase
    dropped = []
    monkeypatch.setattr(ContainerBase, "_drop_all_tables",
                        staticmethod(lambda uri: dropped.append(uri)))
    my = tdx.systems.get("mysql")
    from tabsdatax.itest.tdksystems.systems.base import DomainContext
    my.reset_domain(DomainContext("mysql", "d", {"SQLALCHEMY_URI": "mysql://u:p@h/d"}))
    assert dropped == ["mysql://u:p@h/d"]


def test_reset_domain_unsupported_for_object_store():
    from tabsdatax.itest.tdksystems.systems.base import DomainContext, Unsupported
    # Object stores have no per-domain creds; reset falls back to re-provision.
    with pytest.raises(Unsupported):
        tdx.systems.get("s3").reset_domain(DomainContext("s3", "d", {}))


# ---- create --model --data: multi-format staging + entities + entity term --

def test_domain_create_stages_all_formats_and_records_entities(
    stores, fs_root, monkeypatch
):
    # file/az/gcs/s3 domains stage EVERY supported file format (each under its
    # own subdir), record per-entity record counts, and carry the redundant
    # system-terminology entity-container key (fs -> FOLDER).
    from tabsdatax.itest.tdksystems import cli
    monkeypatch.setattr(cli.env_check, "check_requirements", lambda: None)
    tdx._sys_create(["--type", "fs", "--sys", "a", "--sys-path", str(fs_root), "-f"])
    assert tdx._domain_create(
        ["--sys", "a", "--domain", "inbox", "--domain-prefix", "",
         "--model", "world", "--data", "xs", "-f"]
    ) == 0
    # the domain is named by --domain verbatim; --model/--data only seed it.
    dom_name = "inbox"
    dom = fs_root / dom_name
    assert (dom / "csv" / "wld_city.csv").is_file()
    # JSON is staged as NDJSON (one object per line), not a single array.
    ndjson_file = dom / "ndjson" / "wld_city.ndjson"
    assert ndjson_file.is_file()
    assert not (dom / "json").exists()
    lines = [ln for ln in ndjson_file.read_text().splitlines() if ln.strip()]
    assert len(lines) == 100
    assert json.loads(lines[0])  # each line is a standalone JSON object
    assert (dom / "parquet" / "wld_city.parquet").is_file()
    # the emitter's manifest + the SQL-load run-order manifest are not staged.
    assert not list(dom.rglob("_entities.json"))
    assert not list(dom.rglob("run-order.txt"))
    attrs = descriptor.attributes(_read(store.domain_path(dom_name)))
    assert attrs["FOLDER"] == dom_name
    # entities are keyed by each staged file's full path (one per table+format).
    ents = attrs["ENTITIES"]
    assert ents[f"{dom}/csv/wld_city.csv"] == 100
    assert ents[f"{dom}/ndjson/wld_city.ndjson"] == 100
    assert ents[f"{dom}/parquet/wld_city.parquet"] == 100
    assert len(ents) == 9  # 3 tables x 3 formats


# ---- system --sys-manifest / dump subcommand; domain --auto seeding --------

def test_sys_create_from_manifest(stores, fs_root, tmp_path):
    m = tmp_path / "sys.yaml"
    m.write_text(
        f"- type: fs\n  name: alpha\n  sys-path: {fs_root}/alpha\n  force: true\n"
        f"- type: fs\n  name: beta\n  sys-path: {fs_root}/beta\n  force: true\n"
    )
    assert tdx._sys_create(["--sys-manifest", str(m)]) == 0
    assert {n for n, _ in tdx._store_entries(store.system_dir(), "sys")} == {
        "alpha", "beta"
    }


def test_sys_dump_subcommand_writes_store(stores, fs_root, tmp_path):
    # `tdx-system dump --sys-manifest F` emits a manifest of every system in the
    # store (identity under `name`) that round-trips back through `create`.
    from tabsdatax.itest.tdksystems import manifest
    tdx._sys_create(["--type", "fs", "--sys", "alpha",
                     "--sys-path", str(fs_root / "alpha"), "-f"])
    tdx._sys_create(["--type", "fs", "--sys", "beta",
                     "--sys-path", str(fs_root / "beta"), "-f"])
    out = tmp_path / "systems.yaml"
    assert tdx._sys_dump(["--sys-manifest", str(out)]) == 0
    entries = manifest.read(str(out))
    assert {e["name"] for e in entries} == {"alpha", "beta"}
    assert all(e["type"] == "fs" and "sys-path" in e for e in entries)


def test_sys_dump_subcommand_registered():
    assert "dump" in tdx._SYS_VERBS


def test_sys_create_manifest_rejects_sys(stores):
    with pytest.raises(DatagenError, match="not used with --sys-manifest"):
        tdx._sys_create(["--sys-manifest", "x.yaml", "--sys", "a"])


def test_domain_create_auto_names_and_seeds(
    stores, fs_root, tmp_path, monkeypatch
):
    # --auto creates one domain per system, named after its system, seeding
    # --model/--data where the system supports it.
    from tabsdatax.itest.tdksystems import cli
    monkeypatch.setattr(cli.env_check, "check_requirements", lambda: None)
    tdx._sys_create(["--type", "fs", "--sys", "one",
                     "--sys-path", str(fs_root / "one"), "-f"])
    tdx._sys_create(["--type", "fs", "--sys", "two",
                     "--sys-path", str(fs_root / "two"), "-f"])
    rc = tdx._domain_create(["--auto", "ALL", "--model", "world",
                             "--data", "xs", "--domain-prefix", "", "-f"])
    assert rc == 0
    # each domain is named after its system.
    assert {n for n, _ in tdx._store_entries(store.domain_dir(), "env")} == {
        "one", "two"
    }
    # and each was seeded (data staged in per-format subdirs).
    assert (fs_root / "one" / "one" / "csv" / "wld_city.csv").is_file()
    assert (fs_root / "two" / "two" / "parquet" / "wld_city.parquet").is_file()


def test_domain_create_auto_model_is_noop_for_unsupported_system(
    stores, fs_root, tmp_path, monkeypatch, capsys
):
    # A system that can't be seeded: the domain is still created (empty), the
    # model/data is a no-op, and a warning is emitted to stderr.
    from tabsdatax.itest.tdksystems import cli
    monkeypatch.setattr(cli.env_check, "check_requirements", lambda: None)
    tdx._sys_create(["--type", "fs", "--sys", "flat",
                     "--sys-path", str(fs_root / "flat"), "-f"])
    monkeypatch.setattr(tdx, "_system_can_seed", lambda name, model, sys_dir: False)
    capsys.readouterr()
    rc = tdx._domain_create(["--auto", "ALL", "--model", "world",
                             "--data", "xs", "--domain-prefix", "", "-f"])
    cap = capsys.readouterr()
    assert rc == 0
    # domain named after the system, no data staged.
    assert store.domain_path("flat").exists()
    assert (fs_root / "flat" / "flat").is_dir()
    assert not list((fs_root / "flat" / "flat").rglob("*.csv"))
    # warned that it was created empty.
    assert "flat: model 'world' is not seedable here" in cap.err
    assert "empty (model 'world' not seeded)" in cap.out


def test_domain_create_auto_rejects_sys(stores):
    with pytest.raises(DatagenError, match="not used with --auto"):
        tdx._domain_create(["--auto", "ALL", "--sys", "a"])


def test_system_can_seed_requires_ddl_for_sql(stores):
    # A SQL system is seedable only if the model ships its <slug>.ddl: world has
    # mysql.ddl but no clickhouse.ddl, so clickhouse can't be seeded (empty domain).
    descriptor.write_container_system(
        out=str(store.system_path("my")), system_type="mysql", system_kind="sql",
        label="MySQL", container="c", host="h", port="3306",
        user="root", password="pw", domain_desc="")
    descriptor.stamp(store.system_path("my"), name="my", started="t")
    descriptor.write_container_system(
        out=str(store.system_path("ch")), system_type="clickhouse",
        system_kind="sql", label="ClickHouse", container="c2", host="h",
        port="9000", user="default", password="pw", domain_desc="")
    descriptor.stamp(store.system_path("ch"), name="ch", started="t")
    assert tdx._system_can_seed("my", "world", None) is True
    assert tdx._system_can_seed("ch", "world", None) is False


def test_domain_create_from_manifest(stores, fs_root, tmp_path, monkeypatch):
    # A tdx-domain manifest: each entry's keys mirror the create flags. The entity
    # prefix comes from the CLI (not the manifest), so entities are predictable.
    from tabsdatax.itest.tdksystems import cli
    monkeypatch.setattr(cli.env_check, "check_requirements", lambda: None)
    tdx._sys_create(["--type", "fs", "--sys", "mysys",
                     "--sys-path", str(fs_root), "-f"])
    m = tmp_path / "domains.yaml"
    m.write_text(
        "- name: orders\n  sys: mysys\n  model: world\n  data: xs\n"
        "- name: inbox\n  sys: mysys\n"
    )
    assert tdx._domain_create(
        ["--domain-manifest", str(m), "--domain-prefix", "", "-f"]
    ) == 0
    assert {n for n, _ in tdx._store_entries(store.domain_dir(), "env")} == {
        "orders", "inbox"
    }
    assert (fs_root / "orders" / "csv" / "wld_city.csv").is_file()
    assert (fs_root / "inbox").is_dir()


def test_domain_dump_subcommand_round_trips(stores, fs_root, tmp_path, monkeypatch):
    # `tdx-domain dump --domain-manifest F` dumps every domain (name + sys +
    # model/data, no prefix) so it replays through `create --domain-manifest`.
    from tabsdatax.itest.tdksystems import cli, manifest
    monkeypatch.setattr(cli.env_check, "check_requirements", lambda: None)
    tdx._sys_create(["--type", "fs", "--sys", "mysys",
                     "--sys-path", str(fs_root), "-f"])
    tdx._domain_create(["--sys", "mysys", "--domain", "orders", "--model", "world",
                        "--data", "xs", "--domain-prefix", "", "-f"])
    tdx._domain_create(["--sys", "mysys", "--domain", "inbox",
                        "--domain-prefix", "", "-f"])
    out = tmp_path / "domains.yaml"
    assert tdx._domain_dump(["--domain-manifest", str(out)]) == 0
    entries = {e["name"]: e for e in manifest.read(str(out))}
    assert set(entries) == {"orders", "inbox"}
    assert entries["orders"] == {"name": "orders", "sys": "mysys", "model": "world",
                                 "data": "xs"}
    assert entries["inbox"] == {"name": "inbox", "sys": "mysys"}


def test_domain_dump_subcommand_registered():
    assert "dump" in tdx._DOMAIN_VERBS


# ---- destroy from a manifest (systems + domains) ---------------------------

def test_sys_destroy_from_manifest_destroys_and_skips_missing(
    stores, fs_root, tmp_path, capsys
):
    # Every system in the manifest is destroyed; one not in the store is warned
    # to stderr and skipped (the rest still run).
    tdx._sys_create(["--type", "fs", "--sys", "one",
                     "--sys-path", str(fs_root / "one"), "-f"])
    tdx._sys_create(["--type", "fs", "--sys", "two",
                     "--sys-path", str(fs_root / "two"), "-f"])
    m = tmp_path / "sys.yaml"
    m.write_text("- type: fs\n  name: one\n- type: fs\n  name: two\n"
                 "- type: fs\n  name: ghost\n")
    capsys.readouterr()
    rc = tdx._sys_destroy(["--sys-manifest", str(m)])
    err = capsys.readouterr().err
    assert rc == 0
    assert not store.system_path("one").exists()
    assert not store.system_path("two").exists()
    assert "skipping system 'ghost'" in err


def test_sys_status_from_manifest_reports_and_skips_missing(
    stores, fs_root, tmp_path, capsys
):
    # status accepts a system manifest too (like destroy); a system not in the
    # store is warned to stderr and skipped.
    tdx._sys_create(["--type", "fs", "--sys", "one",
                     "--sys-path", str(fs_root / "one"), "-f"])
    m = tmp_path / "sys.yaml"
    m.write_text("- type: fs\n  name: one\n- type: fs\n  name: ghost\n")
    capsys.readouterr()
    rc = tdx._sys_status(["--sys-manifest", str(m)])
    cap = capsys.readouterr()
    assert rc == 0
    assert "one" in cap.out
    assert "skipping system 'ghost'" in cap.err


def test_sys_destroy_from_manifest_refuses_system_in_use(
    stores, fs_root, tmp_path, capsys
):
    # A system still referenced by a domain (via the domain's sys_name) fails and
    # is left in place.
    tdx._sys_create(["--type", "fs", "--sys", "busy",
                     "--sys-path", str(fs_root), "-f"])
    tdx._domain_create(["--sys", "busy", "--domain", "d",
                        "--domain-prefix", "", "-f"])
    m = tmp_path / "sys.yaml"
    m.write_text("- type: fs\n  name: busy\n")
    capsys.readouterr()
    rc = tdx._sys_destroy(["--sys-manifest", str(m)])
    out = capsys.readouterr().out
    assert rc == 1
    assert "in use by domain(s): d" in out
    assert store.system_path("busy").exists()


def test_domain_destroy_from_manifest_destroys_and_skips_missing(
    stores, fs_root, tmp_path, capsys
):
    # Every domain in the manifest is dropped (folder + descriptor); one whose
    # derived name isn't in the store is warned to stderr and skipped.
    tdx._sys_create(["--type", "fs", "--sys", "mysys",
                     "--sys-path", str(fs_root), "-f"])
    tdx._domain_create(["--sys", "mysys", "--domain", "keep",
                        "--domain-prefix", "", "-f"])
    m = tmp_path / "domains.yaml"
    m.write_text("- name: keep\n  sys: mysys\n- name: ghost\n  sys: mysys\n")
    capsys.readouterr()
    rc = tdx._domain_destroy(["--domain-manifest", str(m)])
    err = capsys.readouterr().err
    assert rc == 0
    assert not store.domain_path("keep").exists()
    assert not (fs_root / "keep").exists()
    assert "skipping domain 'ghost'" in err


# ---- destroy uses the system connection (via the domain's sys_name) ---------

def test_domain_meta_via_system_refreshes_cloud_connection(stores, fs_root):
    tdx._sys_create(["--type", "fs", "--sys", "mysys",
                     "--sys-path", str(fs_root), "-f"])
    # a domain doc with a STALE SYS_PATH but a valid sys_name -> the merge pulls
    # the connection from the live system, keeping the domain's own BASE_PATH.
    doc = {"system": "fs", "name": "d", "sys_name": "mysys",
           "meta": [{"name": "SYS_PATH", "value": "/stale/root"},
                    {"name": "BASE_PATH", "value": "/stale/root/d"}]}
    merged = tdx._domain_meta_via_system(doc, None)
    assert merged["SYS_PATH"] == str(fs_root)        # refreshed from the system
    assert merged["BASE_PATH"] == "/stale/root/d"    # entity identity kept


def test_domain_meta_via_system_maps_container_admin(stores):
    descriptor.write_container_system(
        out=str(store.system_path("db")), system_type="mysql", system_kind="sql",
        label="MySQL", container="tdx_db", host="h", port="3306",
        user="root", password="rootpw", domain_desc="")
    descriptor.stamp(store.system_path("db"), name="db", started="t")
    doc = {"system": "mysql", "name": "shop", "sys_name": "db",
           "meta": [{"name": "DB", "value": "shop"},
                    {"name": "HOST", "value": "oldhost"},
                    {"name": "PORT", "value": "1"},
                    {"name": "SYS_ADMIN_USER", "value": "stale"},
                    {"name": "SYS_ADMIN_PASSWORD", "value": "stalepw"}]}
    merged = tdx._domain_meta_via_system(doc, None)
    # admin creds refreshed from the system's own USER / PASSWORD / HOST / PORT.
    assert merged["SYS_ADMIN_USER"] == "root"
    assert merged["SYS_ADMIN_PASSWORD"] == "rootpw"
    assert merged["HOST"] == "h" and merged["PORT"] == "3306"
    assert merged["DB"] == "shop"                    # entity identity kept


def test_domain_meta_via_system_falls_back_without_sys_name():
    doc = {"system": "fs", "name": "d",
           "meta": [{"name": "SYS_PATH", "value": "/x"}]}
    assert tdx._domain_meta_via_system(doc, None) == {"SYS_PATH": "/x"}


def test_domain_destroy_uses_system_and_removes_folder(stores, fs_root):
    tdx._sys_create(["--type", "fs", "--sys", "mysys",
                     "--sys-path", str(fs_root), "-f"])
    tdx._domain_create(["--sys", "mysys", "--domain", "d", "--domain-prefix", "", "-f"])
    assert (fs_root / "d").is_dir()
    tdx._domain_destroy(["--domain", "d"])
    assert not store.domain_path("d").exists()
    assert not (fs_root / "d").exists()


def test_domain_create_single_defaults_name_to_system(stores, fs_root):
    # --sys with no --domain -> domain named after the system.
    tdx._sys_create(["--type", "fs", "--sys", "mysys",
                     "--sys-path", str(fs_root), "-f"])
    tdx._domain_create(["--sys", "mysys", "--domain-prefix", "", "-f"])
    assert store.domain_path("mysys").exists()
    assert (fs_root / "mysys").is_dir()


def test_manifest_batch_reports_per_entry_failure(stores, fs_root, tmp_path, capsys):
    # A bad entry (unknown type) is reported; the good one still runs.
    m = tmp_path / "mixed.yaml"
    m.write_text(
        f"- type: fs\n  sys: good\n  sys-path: {fs_root}/good\n  force: true\n"
        f"- type: nope\n  sys: bad\n"
    )
    capsys.readouterr()
    rc = tdx._sys_create(["--sys-manifest", str(m)])
    out = capsys.readouterr().out
    assert rc == 1                       # one entry failed
    assert "created 1/2 systems" in out
    assert "good" in out and "bad" in out


# ---- fs: --sys-path is a local option (no env), default target/<name> ------

def test_fs_default_sys_path_is_target_backends_name(stores, tmp_path, monkeypatch):
    # No --sys-path -> root defaults to target/backends/<system name> under cwd;
    # no $FS__SYS_PATH involved.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("FS__SYS_PATH", "/tmp/should-be-ignored")
    assert tdx._sys_create(["--type", "fs", "--sys", "myfs", "-f"]) == 0
    doc = _read(store.system_path("myfs"))
    assert descriptor.attributes(doc)["SYS_PATH"] == "target/backends/myfs"
    assert (tmp_path / "target" / "backends" / "myfs").is_dir()


def test_fs_create_has_no_env_options(stores):
    # fs offers only --sys-path (no --env:sys-path / --env-prefix cloud machinery).
    from tabsdatax.itest.tdksystems.shell import ArgumentParser
    p = ArgumentParser(prog="tdx-system create --type fs")
    tdx.systems.get("fs").add_create_options(p)
    flags = {a for action in p._actions for a in action.option_strings}
    assert "--sys-path" in flags
    assert "--env:sys-path" not in flags and "--env-prefix" not in flags


# ---- azureblob default env prefix is AZ ------------------------------------

def test_azureblob_default_env_prefix_is_az():
    import argparse
    az = tdx.systems.get("azureblob")
    assert az.default_env_prefix == "AZ"
    settings = az.resolve_create_settings(argparse.Namespace(env_prefix="AZ"))
    assert settings["CONTAINER"].env_var == "AZ__CONTAINER"
    assert settings["ACCOUNT_KEY"].env_var == "AZ__ACCOUNT_KEY"


# ---- DOMAIN_PREFIX: the entity container's name in the external system -----

def test_resolve_domain_prefix_precedence(monkeypatch):
    # --domain-prefix wins verbatim (even empty, meaning "no prefix").
    assert tdx._resolve_domain_prefix("myrun_") == "myrun_"
    assert tdx._resolve_domain_prefix("") == ""
    # else $TD_BUILD_USER / $TD_BUILD_NUMBER ...
    monkeypatch.setenv("TD_BUILD_USER", "ci")
    monkeypatch.setenv("TD_BUILD_NUMBER", "7")
    assert tdx._resolve_domain_prefix(None) == "ci_7_"
    # ... each sanitised to identifier-safe tokens.
    monkeypatch.setenv("TD_BUILD_USER", "a.b/c")
    assert tdx._resolve_domain_prefix(None) == "a_b_c_7_"
    # ... else the OS user + build 0.
    monkeypatch.delenv("TD_BUILD_USER", raising=False)
    monkeypatch.delenv("TD_BUILD_NUMBER", raising=False)
    monkeypatch.setattr(tdx, "_os_username", lambda: "devx")
    assert tdx._resolve_domain_prefix(None) == "devx_0_"


def test_ident_token_sanitizes():
    assert tdx._ident_token("tucu") == "tucu"
    assert tdx._ident_token("a.b/c") == "a_b_c"
    assert tdx._ident_token("--x--") == "x"
    assert tdx._ident_token("") == "x"          # never empty (would break names)


def test_domain_prefix_defaults_to_build_identity(stores, fs_root, monkeypatch):
    # No --domain-prefix -> the entity dir is named <TD_BUILD_USER>_<NUMBER>_<domain>;
    # the descriptor's logical name stays the bare domain name.
    monkeypatch.setenv("TD_BUILD_USER", "ci")
    monkeypatch.setenv("TD_BUILD_NUMBER", "7")
    tdx._sys_create(["--type", "fs", "--sys", "my_fs",
                     "--sys-path", str(fs_root), "-f"])
    tdx._domain_create(["--sys", "my_fs", "--domain", "inbox", "-f"])
    doc = _read(store.domain_path("inbox"))
    assert doc["name"] == "inbox"                       # logical name unchanged
    assert doc["external_name"] == "ci_7_inbox"
    assert (fs_root / "ci_7_inbox").is_dir()            # the real entity dir
    assert not (fs_root / "inbox").exists()
    info = json.loads(descriptor.info(doc, "json"))
    assert info["NAME"] == "inbox" and info["EXTERNAL_NAME"] == "ci_7_inbox"
    assert info["BASE_PATH"] == f"{fs_root}/ci_7_inbox"


def test_domain_prefix_falls_back_to_os_user(stores, fs_root, monkeypatch):
    monkeypatch.delenv("TD_BUILD_USER", raising=False)
    monkeypatch.delenv("TD_BUILD_NUMBER", raising=False)
    monkeypatch.setattr(tdx, "_os_username", lambda: "devx")
    tdx._sys_create(["--type", "fs", "--sys", "my_fs",
                     "--sys-path", str(fs_root), "-f"])
    tdx._domain_create(["--sys", "my_fs", "--domain", "inbox", "-f"])
    assert _read(store.domain_path("inbox"))["external_name"] == "devx_0_inbox"
    assert (fs_root / "devx_0_inbox").is_dir()


def test_domain_prefix_explicit_option(stores, fs_root):
    tdx._sys_create(["--type", "fs", "--sys", "my_fs",
                     "--sys-path", str(fs_root), "-f"])
    tdx._domain_create(["--sys", "my_fs", "--domain", "inbox",
                        "--domain-prefix", "run5_", "-f"])
    assert _read(store.domain_path("inbox"))["external_name"] == "run5_inbox"
    assert (fs_root / "run5_inbox").is_dir()


def test_recreate_reuses_recorded_external_name(stores, fs_root, monkeypatch):
    # A domain created under one build identity keeps that entity name on
    # recreate — even if the build number has since changed — so recreate
    # rebuilds the SAME external resource, not a freshly-prefixed one.
    monkeypatch.setenv("TD_BUILD_USER", "ci")
    monkeypatch.setenv("TD_BUILD_NUMBER", "7")
    tdx._sys_create(["--type", "fs", "--sys", "a",
                     "--sys-path", str(fs_root), "-f"])
    tdx._domain_create(["--sys", "a", "--domain", "inbox", "-f"])
    assert (fs_root / "ci_7_inbox").is_dir()
    # The build advances; recreate must still target ci_7_inbox.
    monkeypatch.setenv("TD_BUILD_NUMBER", "9")
    assert tdx._domain_recreate(["--domain", "inbox"]) == 0
    assert (fs_root / "ci_7_inbox").is_dir()
    assert not (fs_root / "ci_9_inbox").exists()
    assert _read(store.domain_path("inbox"))["external_name"] == "ci_7_inbox"
