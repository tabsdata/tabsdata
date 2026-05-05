#
# Copyright 2026 Tabs Data Inc.
#
"""AWS S3 external system for live integration tests.

Gated by ``$S3`` — tests run unless ``$S3`` is set to ``"skip"``
(case-insensitive). When a ``prefix`` override is passed to
:func:`external`, the prefix replaces ``S3`` both as the gate env
var and as the base for every suffix below. Conventional bundle
that ``s3.sh`` exports:

* ``S3__BUCKET``        — bucket name
* ``S3__REGION``        — AWS region (optional)
* ``S3__ACCESS_KEY``    — AWS access key ID
* ``S3__SECRET_KEY``    — AWS secret access key
* ``S3__ENDPOINT_URL``  — optional endpoint override
  (set when running against MinIO / localstack)
"""

from __future__ import annotations

from tabsdata_testutils.external import ExternalSystem


class S3(ExternalSystem):
    @classmethod
    def system_env_name(cls) -> str:
        return "S3"

    @classmethod
    def env_names(cls) -> list[str]:
        return ["BUCKET", "REGION", "ACCESS_KEY", "SECRET_KEY", "ENDPOINT_URL"]
