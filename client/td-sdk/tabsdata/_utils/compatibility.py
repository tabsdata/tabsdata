#
# Copyright 2025 Tabs Data Inc.
#

import platform
import subprocess
import sys
import warnings
from importlib.metadata import PackageNotFoundError, requires, version

from tabsdata._utils.constants import POLARS_MODULE_NAME, TABSDATA_MODULE_NAME

STICKY_VERSION_PACKAGES = [POLARS_MODULE_NAME]


class CompatibilityError(RuntimeError):
    def __init__(self, messages):
        if isinstance(messages, str):
            messages = [messages]
        self.messages = messages
        super().__init__("\n".join(messages))


class PackageVersionError(RuntimeError):
    def __init__(self, messages):
        if isinstance(messages, str):
            messages = [messages]
        self.messages = messages
        super().__init__("\n".join(messages))


# noinspection PyBroadException
def check_sticky_version_packages() -> None:
    tabsdata_package_requires = requires(TABSDATA_MODULE_NAME)
    for package in STICKY_VERSION_PACKAGES:
        try:
            check_sticky_version_package(package, tabsdata_package_requires)
        except PackageVersionError:
            raise
        except Exception:
            pass


def check_sticky_version_package(package: str, tabsdata_package_requires) -> None:
    if not tabsdata_package_requires:
        return
    package_required_version = None
    for tabsdata_package_require in tabsdata_package_requires:
        if tabsdata_package_require.startswith(f"{package}=="):
            package_required_version = (
                tabsdata_package_require.split("==")[1].split(";")[0].strip()
            )
            break
    if package_required_version is None:
        return
    try:
        package_current_version = version(package)
    except PackageNotFoundError:
        raise PackageVersionError(
            f"Package '{package}' is required but not installed.\n"
            "Please install it with:\n"
            f"pip install {package}=={package_required_version}"
        )
    if package_current_version != package_required_version:
        raise PackageVersionError(
            f"Package '{package}' version mismatch: "
            f"required {package_required_version} "
            "but "
            f"found {package_current_version}.\n"
            "Please use the exact version with:\n"
            f"pip install {package}=={package_required_version}"
        )


def check_load() -> None:
    smoke_code = """
import sys
try:
    import polars as pl
    pl.DataFrame({"o": ["aleph"]}).lazy().collect()
    sys.exit(0)
except Exception as e:
    print(f"Import error: {e}", file=sys.stderr)
    sys.exit(1)
"""
    check_messages = []
    try:
        result = subprocess.run(
            [sys.executable, "-c", smoke_code],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode < 0:
            import signal

            signal_name = signal.Signals(-result.returncode).name
            check_messages.append("")
            check_messages.append(
                f"Load crashed with signal {signal_name} (code {result.returncode})"
            )
            check_messages.append(
                "This typically means the cpu lacks required instruction sets."
            )
            raise CompatibilityError(check_messages)
        elif result.returncode != 0:
            check_messages.append("")
            check_messages.append("Failed to load.")
            if result.stderr:
                check_messages.append(result.stderr.strip())
            raise CompatibilityError(check_messages)
    except subprocess.TimeoutExpired:
        check_messages.append("")
        check_messages.append("Load timed out.")
        check_messages.append("This may indicate cpu compatibility issues.")
        raise CompatibilityError(check_messages)
    except CompatibilityError:
        raise
    except Exception as exception:
        check_messages.append("")
        check_messages.append("Failed to smoke test.")
        check_messages.append(str(exception).strip())
        raise CompatibilityError(check_messages)


def check_cpu() -> None:
    from polars._cpu_check import check_cpu_flags

    check_messages = []
    with warnings.catch_warnings(record=True) as warning_outputs:
        warnings.simplefilter("always", RuntimeWarning)
        check_cpu_flags("")
        if warning_outputs:
            for warning_output in warning_outputs:
                if issubclass(warning_output.category, RuntimeWarning):
                    check_messages.append("")
                    warning_messages = str(warning_output.message)
                    check_messages.append(
                        "This cpu lacks required features for "
                        "tabsdata standard runtime."
                    )
                    check_messages.append("")
                    for warning_message in warning_messages.split(""):
                        check_messages.append(warning_message.strip())
                    raise CompatibilityError(check_messages)


def check_lib() -> None:
    check_messages = []
    try:
        import polars as pl

        pl.DataFrame({"o": ["aleph"]}).lazy().collect()
    except Exception as exception:
        exception_messages = str(exception)
        for exception_message in exception_messages.split(""):
            check_messages.append(exception_message.strip())
        raise CompatibilityError(check_messages)


# noinspection PyListCreation
def sys_info():
    import polars as pl
    from polars._cpu_check import get_runtime_repr

    info_messages = []
    try:
        info_messages.append("")
        info_messages.append("System Information:")
        info_messages.append(f"  - Platform.............: {platform.platform()}")
        info_messages.append(
            f"  - Processor............: {platform.processor() or 'Unknown'}"
        )
        info_messages.append(
            f"  - Machine..............: {platform.machine() or 'Unknown'}"
        )
        info_messages.append(f"  - Python Version.......: {sys.version.split()[0]}")
        info_messages.append(
            f"  - Python Implementation: {platform.python_implementation()}"
        )
        info_messages.append(f"  - Polars Version.......: {pl.__version__}")
        # noinspection PyBroadException
        try:
            runtime = get_runtime_repr()
            info_messages.append(f"  - Polars Runtime.......: {runtime}")
        except Exception:
            info_messages.append("  - Polars Runtime.......: Not yet loaded")
    except Exception as exception:
        exception_messages = str(exception)
        info_messages.append("")
        info_messages.append("Cannot retrieve complete system information.")
        for exception_message in exception_messages.split(""):
            info_messages.append(exception_message.strip())
    return info_messages


if __name__ == "__main__":  # noqa: C901
    validation_messages = []
    try:
        check_load()
    except CompatibilityError as load_error:
        validation_messages.extend(load_error.messages)
    if not validation_messages:
        try:
            check_cpu()
        except CompatibilityError as cpu_error:
            validation_messages.extend(cpu_error.messages)
        try:
            check_lib()
        except CompatibilityError as lib_error:
            validation_messages.extend(lib_error.messages)

    if validation_messages:
        from colorama import Fore, Style, init

        validation_messages = sys_info() + validation_messages
        init()
        print(
            Fore.RED,
            file=sys.stderr,
        )
        print(
            "The cpu on this system is not supported.",
            file=sys.stderr,
        )
        print(
            "",
            file=sys.stderr,
        )
        print(
            "If you are running inside a virtual machine or container, the emulated ",
            file=sys.stderr,
        )
        print(
            "processor may not fully support the simd instruction sets required by ",
            file=sys.stderr,
        )
        print(
            "tabsdata.",
            file=sys.stderr,
        )
        print(
            "",
            file=sys.stderr,
        )
        print(
            "Run tabsdata on a modern cpu with full simd support.",
            file=sys.stderr,
        )
        print(
            "",
            file=sys.stderr,
        )
        print(
            "Refer to the diagnostic messages below for more details.",
            file=sys.stderr,
        )
        for message in validation_messages:
            print(
                message,
                file=sys.stderr,
            )
        print(
            Style.RESET_ALL,
            file=sys.stderr,
        )
        sys.exit(1)
    else:
        try:
            check_sticky_version_packages()
        except PackageVersionError as error:
            print(error.messages, file=sys.stderr)
