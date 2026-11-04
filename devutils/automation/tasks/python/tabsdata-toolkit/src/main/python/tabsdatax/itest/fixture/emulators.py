#
# Copyright 2026 Tabsdata Inc.
#

"""The three cloud stores a storage test runs on when it is not about one of
them: their local emulators for unit tests, the real clouds for itests. A test
takes all three ``domain_<name>`` fixtures and is parametrized over the names."""

from __future__ import annotations

from collections.abc import Callable

from tabsdatax.itest.fixture.backends.azureblob import azureblob_domain
from tabsdatax.itest.fixture.backends.azurebloblocal import azurebloblocal_domain
from tabsdatax.itest.fixture.backends.gcs import gcs_domain
from tabsdatax.itest.fixture.backends.gcslocal import gcslocal_domain
from tabsdatax.itest.fixture.backends.s3 import s3_domain
from tabsdatax.itest.fixture.backends.s3local import s3local_domain

EMULATORS = ("azurebloblocal", "gcslocal", "s3local")
CLOUDS = ("azureblob", "gcs", "s3")


def declaring(*declarations: tuple[Callable, str]) -> Callable:
    def declare(test: Callable) -> Callable:
        for declaration, name in declarations:
            test = declaration(name)(test)
        return test

    return declare


emulated = declaring(
    (azurebloblocal_domain, "azurebloblocal"),
    (gcslocal_domain, "gcslocal"),
    (s3local_domain, "s3local"),
)

clouded = declaring(
    (azureblob_domain, "azureblob"),
    (gcs_domain, "gcs"),
    (s3_domain, "s3"),
)
