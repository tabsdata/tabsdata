#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
    TabsdataException,
)


class PyenvException(TabsdataException): ...


class PyenvInitException(PyenvException): ...


class PyenvRuntimeException(PyenvException): ...


class PyenvInternalException(PyenvException): ...


class UnsupportedInstallMethodError(PyenvRuntimeException): ...


class PackageNotInstalledError(PyenvRuntimeException): ...


class FeatureDetectionError(PyenvRuntimeException): ...


class VenvCreationError(PyenvRuntimeException): ...


class BinaryNotFoundError(PyenvRuntimeException): ...


class WheelFileMissingError(PyenvRuntimeException): ...


class PyenvErrorCode(ErrorCode):
    PYENV_1 = ErrorDef(
        UnsupportedInstallMethodError,
        "Install method {recipe!r} for package {package!r} is not supported for "
        "runtime mirroring. Supported methods: {supported}.",
    )
    PYENV_2 = ErrorDef(
        PackageNotInstalledError,
        "Mandatory package {package!r} is not installed in the current environment.",
    )
    PYENV_3 = ErrorDef(
        FeatureDetectionError,
        "Could not determine tabsdata edition: package {package!r} has no resolvable "
        "location (likely installed from a registry).",
    )
    PYENV_4 = ErrorDef(
        VenvCreationError,
        "Venv creation step failed: {detail}",
    )
    PYENV_5 = ErrorDef(
        BinaryNotFoundError,
        "Binary {name!r} not found in the active environment's scripts folder "
        "({scripts}).",
    )
    PYENV_6 = ErrorDef(
        WheelFileMissingError,
        "Wheel file for package {package!r} is no longer present at {location} "
        "(the host environment installed it from a wheel that has since been "
        "deleted). Reinstall {package!r} in the host environment to repopulate "
        "the wheel, or switch to an editable / registry install.",
    )
