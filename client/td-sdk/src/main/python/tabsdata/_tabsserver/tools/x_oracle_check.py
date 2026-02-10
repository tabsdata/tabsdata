#
# Copyright 2025 Tabs Data Inc.
#

import os
import platform
import re
import sys

import rich_click as click
from rich.console import Console
from rich.text import Text

console = Console()


def print_dpi1047(message: str) -> bool:
    if "DPI-1047" not in message:
        return False

    tried_index = message.find("tried:")
    if tried_index == -1:
        return False

    before_tried = message[:tried_index].strip()
    console.print(f"      {before_tried}")

    tried_text = message[tried_index:]
    tried_entries = re.findall(r"'([^']+)'[ \t]*\(([^)]+)\)", tried_text)
    for path, reason in tried_entries:
        console.print(f"        • '{path}' ({reason})")

    last_paren = tried_text.rfind(")")
    if last_paren != -1 and last_paren + 1 < len(tried_text):
        tail = tried_text[last_paren + 1 :].strip()
        if tail:
            console.print(f"      {tail}")

    return True


# noinspection DuplicatedCode
def check_package_oracledb() -> bool:  # noqa: C901
    console.print("")
    console.print("2.- Testing oracledb Python package...")

    try:
        # noinspection PyPackageRequirements
        import oracledb

        console.print("    - Package oracledb imported successfully")
        console.print(f"    - Version of oracledb: {oracledb.version}")
        try:
            oracledb.init_oracle_client()
            oracle_instant_client_version = oracledb.clientversion()
            console.print(
                "    - Version of Oracle Instant Client: "
                f" {'.'.join(map(str, oracle_instant_client_version))}"
            )
        except Exception as e:
            console.print("    - Oracle Instant Client version check failed")
            if not print_dpi1047(str(e)):
                console.print(f"      {e}")
            return False
    except ImportError as e:
        console.print(f"    - Package oracledb is not available: {e}")
        console.print("    - You can run 'pip install oracledb' to address this issue.")
        console.print("    - This is only an issue if you have Oracle subscribers.")
        return False
    except Exception as e:
        console.print(f"    - Unexpected error checking package oracledb: {e}")
        return False

    return True


# noinspection DuplicatedCode
def check_package_cx_oracle() -> bool:
    console.print("")
    console.print("1.- Testing cx_Oracle Python package...")

    try:
        # noinspection PyPackageRequirements
        import cx_Oracle

        console.print("    - Package cx_Oracle imported successfully")
        console.print(f"    - Version of cx_Oracle: {cx_Oracle.version}")
        try:
            cx_Oracle.init_oracle_client()
            oracle_instant_client_version = cx_Oracle.clientversion()
            console.print(
                "    - Version of Oracle Instant Client: "
                f" {'.'.join(map(str, oracle_instant_client_version))}"
            )
        except Exception as e:
            console.print("    - Oracle Instant Client version check failed")
            if not print_dpi1047(str(e)):
                console.print(f"      {e}")
            return False
    except ImportError as e:
        console.print(f"    - Package cx_Oracle is not available: {e}")
        console.print(
            "    - You can run 'pip install cx_Oracle' to address this issue."
        )
        console.print(
            "    - This is not an issue, as oracledb is preferred over cx_Oracle."
        )
        return False
    except Exception as e:
        console.print(f"    - Unexpected checking package cx_Oracle: {e}")
        return False

    return True


def check_packages() -> bool:
    cx_oracle_ok = check_package_cx_oracle()
    oracledb_ok = check_package_oracledb()
    if not cx_oracle_ok and not oracledb_ok:
        console.print(
            "Neither cx_Oracle nor oracledb packages are available. "
            "You will need at least one of them."
        )
    return cx_oracle_ok or oracledb_ok


def check_oracle_envs() -> bool:
    console.print("")
    console.print("3.- Checking environment variables...")

    system = platform.system().lower()
    oracle_env_vars = {}
    if system == "linux":
        oracle_env_vars["LD_LIBRARY_PATH"] = "Library search path"
    elif system == "darwin":
        oracle_env_vars["DYLD_LIBRARY_PATH"] = "Library search path"
    oracle_env_vars["ORACLE_BASE"] = "Oracle base directory"
    oracle_env_vars["ORACLE_HOME"] = "Oracle installation directory"
    oracle_env_vars["TNS_ADMIN"] = "TNS configuration directory"
    oracle_env_vars["PATH"] = "System PATH"

    for variable, description in oracle_env_vars.items():
        value = os.environ.get(variable)
        if value:
            console.print(f"    - Environment variable {variable} set:")
            for path in value.split(os.pathsep):
                if path.strip():
                    console.print(f"        • {path}")
        else:
            console.print(
                f"    - Environment variable {variable} not set ({description})"
            )

    return True


def check_oracle_libraries() -> bool:  # noqa: C901
    console.print("")
    console.print("4.- Checking Oracle libraries...")

    system = platform.system().lower()
    if system == "linux":
        lib_patterns = [
            "libclntsh.so*",
            "libocci.so*",
        ]
        lib_paths = [
            "/usr/lib",
            "/usr/local/lib",
            "/opt/oracle",
        ]
    elif system == "darwin":
        lib_patterns = [
            "libclntsh.dylib*",
            "libocci.dylib*",
        ]
        lib_paths = [
            "/usr/lib",
            "/usr/local/lib",
            "/opt/oracle",
        ]
    elif system == "windows":
        lib_patterns = [
            "oci.dll",
            "oraociei*.dll",
        ]
        lib_paths = [
            "C:\\oracle",
            "C:\\app\\oracle",
        ]
    else:
        console.print(f"Unsupported system: {system}")
        lib_patterns = []
        lib_paths = []
    oracle_home = os.environ.get("ORACLE_HOME")
    if oracle_home:
        lib_paths.append(os.path.join(oracle_home, "lib"))
        if system == "windows":
            lib_paths.append(os.path.join(oracle_home, "bin"))

    found_libs = []
    for path in lib_paths:
        if os.path.exists(path):
            for pattern in lib_patterns:
                import glob

                matches = glob.glob(os.path.join(path, pattern))
                found_libs.extend(matches)
    if found_libs:
        console.print("    - Oracle libraries found:")
        for lib in found_libs:
            console.print(f"        • {lib}")
    else:
        console.print("    - No Oracle libraries found in standard locations")

    return True


def check_cx_oracle_functionality() -> bool:
    console.print("")
    console.print("5.- Testing Oracle connectivity with cx_oracle...")

    try:
        # noinspection PyPackageRequirements,PyUnresolvedReferences
        import cx_Oracle
    except ImportError:
        console.print("    Driver cx_oracle not available. Skipping test")
        return True

    try:
        dsn = (
            "(DESCRIPTION="
            "(CONNECT_TIMEOUT=5)"
            "(CONNECT_DATA=(SERVICE_NAME=dummy))"
            "(ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT=1521)))"
        )

        try:
            cx_Oracle.connect(
                user="dummy",
                password="dummy",
                dsn=dsn,
            )
            console.print("    - Unexpected: dummy pool creation succeeded!")
        except cx_Oracle.DatabaseError as e:
            error_code = e.args[0].code if e.args else 0
            if error_code in [12154, 12514, 12541, 1017]:
                console.print("    - Oracle client libraries are functional!")
            else:
                console.print(
                    "    - Unexpected database error, but it appears to be "
                    f"functional: {e}"
                )
        except Exception as e:
            console.print(f"    - Oracle client library is not functional: {e}")
            return False
    except Exception as e:
        console.print(f"    - Oracle client library cannot be loaded: {e}")
        return False

    return True


def check_oracledb_functionality() -> bool:
    console.print("")
    console.print("6.- Testing Oracle connectivity with oracledb...")

    try:
        # noinspection PyPackageRequirements,PyUnresolvedReferences
        import oracledb
    except ImportError:
        console.print("    Driver oracledb not available. Skipping test")
        return True

    try:
        dsn = (
            "(DESCRIPTION="
            "(CONNECT_TIMEOUT=5)"
            "(CONNECT_DATA=(SERVICE_NAME=dummy))"
            "(ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT=1521)))"
        )

        try:
            oracledb.connect(
                user="dummy",
                password="dummy",
                dsn=dsn,
            )
            console.print("    - Unexpected: dummy pool creation succeeded!")
        except oracledb.DatabaseError as e:
            error_code = e.args[0].code if e.args else 0
            if error_code in [12154, 12514, 12541, 1017]:
                console.print("    - Oracle client libraries are functional!")
            else:
                console.print(
                    "    - Unexpected database error, but it appears to be "
                    f"functional: {e}"
                )
        except Exception as e:
            console.print(f"    - Oracle client library is not functional: {e}")
            return False
    except Exception as e:
        console.print(f"    - Oracle client library cannot be loaded: {e}")
        return False

    return True


def check_oracle_functionality() -> bool:
    cx_oracle_functionality_ok = check_cx_oracle_functionality()
    oracledb_functionality_ok = check_oracledb_functionality()
    if not cx_oracle_functionality_ok and not oracledb_functionality_ok:
        console.print(
            "Connectivity test failed for cx_Oracle or oracledb packages. "
            "You will need at least one of them fully functional."
        )
    return cx_oracle_functionality_ok or oracledb_functionality_ok


def check_oracle_setup() -> bool:
    return (
        check_packages()
        and check_oracle_envs()
        and check_oracle_libraries()
        and check_oracle_functionality()
    )


@click.command(
    help=(
        "Ensure that both the cx_Oracle Python package and Oracle Instant "
        "Client are correctly installed and properly configured in your "
        "environment."
    ),
    context_settings=dict(help_option_names=["-h", "--help"]),
)
@click.version_option()
def cli():
    console.print(
        Text(
            "\nThis utility is planned for removal in a future release.\n"
            "Refer to the release notes for upcoming versions to find its replacement.",
            style="orange1",
        )
    )

    console.print("")
    console.print(
        "Validating that Oracle dynamic libraries are correctly installed and"
        " configured."
    )
    success = check_oracle_setup()
    if success:
        console.print("")
        console.print(
            "Oracle dynamic libraries appear to be properly installed and configured!"
        )
        console.print("")
    else:
        console.print("")
        console.print(
            "Oracle dynamic libraries are not correctly installed or configured."
        )
        console.print("Refer to the messages above to resolve the issue.")
        console.print("")
        permalink = (
            "https://www.oracle.com/database/technologies/instant-client/downloads.html"
        )
        system = platform.system().lower()
        if system == "linux":
            machine = platform.machine().lower()
            if "x86_64" in machine or "amd64" in machine:
                permalink = (
                    "https://www.oracle.com/"
                    "database/"
                    "technologies/"
                    "instant-client/"
                    "linux-x86-64-downloads.html"
                )
        elif system == "darwin":
            machine = platform.machine().lower()
            if "arm" in machine or "aarch" in machine:
                permalink = (
                    "https://www.oracle.com/"
                    "database/"
                    "technologies/"
                    "instant-client/"
                    "macos-arm64-downloads.html"
                )
            elif "x86_64" in machine or "amd64" in machine:
                permalink = (
                    "https://www.oracle.com/"
                    "database/"
                    "technologies/"
                    "instant-client/"
                    "macos-intel-x86-downloads.html"
                )
        elif system == "windows":
            machine = platform.machine().lower()
            if "x86_64" in machine or "amd64" in machine:
                permalink = (
                    "https://www.oracle.com/"
                    "database/"
                    "technologies/"
                    "instant-client/"
                    "winx64-64-downloads.html"
                )
        console.print(
            Text(
                "\nVisit the link below to download the Oracle Instant Client "
                "and review the installation instructions:\n\n"
                f"{permalink}",
                style="green",
            )
        )
        console.print("")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    cli()
