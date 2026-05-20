#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations


class UnsupportedInstallMethodError(NotImplementedError):
    message = (
        "Install method '{recipe}' for package '{package}' is not supported for"
        " runtime mirroring. Supported methods: {supported}."
    )

    def __init__(self, *, recipe: str, package: str, supported: str) -> None:
        super().__init__(
            self.message.format(recipe=recipe, package=package, supported=supported)
        )


class PackageNotInstalledError(LookupError):
    message = (
        "Mandatory package '{package}' is not installed in the current environment."
    )

    def __init__(self, *, package: str) -> None:
        super().__init__(self.message.format(package=package))


class FeatureDetectionError(RuntimeError):
    message = (
        "Could not determine tabsdata edition: package '{package}' has no resolvable"
        " location (likely installed from a registry)."
    )

    def __init__(self, *, package: str) -> None:
        super().__init__(self.message.format(package=package))


class VenvCreationError(RuntimeError):
    message = "Venv creation step failed: {detail}"

    def __init__(self, *, detail: str) -> None:
        super().__init__(self.message.format(detail=detail))
