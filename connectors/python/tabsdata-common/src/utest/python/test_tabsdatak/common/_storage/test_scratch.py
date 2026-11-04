#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import uuid
from collections.abc import Iterator

import pytest
from tabsdatax.itest.fixture.backends.azurebloblocal import AzureBlobLocalDomain
from tabsdatax.itest.fixture.backends.gcslocal import GcsLocalDomain
from tabsdatax.itest.fixture.backends.s3local import S3LocalDomain
from tabsdatax.itest.fixture.emulators import EMULATORS, emulated

from tabsdatak.common._storage.scratch import CHUNK, ScratchFiles

pytestmark = [pytest.mark.unit]

CONTENT = bytes(range(256)) * ((CHUNK * 2 + 1000) // 256)


@pytest.fixture(params=EMULATORS)
def scratch(
    request,
    domain_azurebloblocal: AzureBlobLocalDomain,
    domain_gcslocal: GcsLocalDomain,
    domain_s3local: S3LocalDomain,
) -> Iterator[ScratchFiles]:
    domain = locals()[f"domain_{request.param}"]
    root = f"{domain.uri.rstrip('/')}/scratch-{uuid.uuid4().hex}"
    files = ScratchFiles(root, dict(domain.storage_options))
    yield files
    files.clear()


@emulated
def test_a_written_file_reads_back_from_any_position(
    domain_azurebloblocal, domain_gcslocal, domain_s3local, scratch
):
    scratch.write("a.bin", io.BytesIO(CONTENT))
    reader = scratch.read("a.bin")
    assert reader.read(10) == CONTENT[:10]
    reader.seek(-5, io.SEEK_END)
    assert reader.read() == CONTENT[-5:]
    reader.seek(CHUNK - 3)
    assert reader.read(6) == CONTENT[CHUNK - 3 : CHUNK + 3]


@emulated
def test_writing_a_name_that_exists_is_refused(
    domain_azurebloblocal, domain_gcslocal, domain_s3local, scratch
):
    scratch.write("a.bin", io.BytesIO(b"first"))
    with pytest.raises(FileExistsError, match="'a.bin' already exists"):
        scratch.write("a.bin", io.BytesIO(b"second"))
    assert scratch.read("a.bin").read() == b"first"


@emulated
def test_replacing_overwrites(
    domain_azurebloblocal, domain_gcslocal, domain_s3local, scratch
):
    scratch.write("a.bin", io.BytesIO(b"first"))
    scratch.replace("a.bin", io.BytesIO(b"second"))
    assert scratch.read("a.bin").read() == b"second"


@emulated
def test_deleting_and_clearing_remove_the_files(
    domain_azurebloblocal, domain_gcslocal, domain_s3local, scratch
):
    for name in ("a.bin", "b.bin", "c.bin"):
        scratch.write(name, io.BytesIO(name.encode()))
    scratch.delete("a.bin")
    with pytest.raises(FileNotFoundError):
        scratch.read("a.bin")
    scratch.clear()
    for name in ("b.bin", "c.bin"):
        with pytest.raises(FileNotFoundError):
            scratch.read(name)


def test_only_file_operations_are_exposed():
    public = {name for name in dir(ScratchFiles) if not name.startswith("_")}
    assert public == {"clear", "delete", "read", "replace", "write"}
