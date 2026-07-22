#
# Copyright 2026 Tabsdata Inc.
#

import argparse
import importlib
import importlib.util
import json
import os
import platform
import subprocess
import sys
from types import ModuleType

from rich import box
from rich.console import Console
from rich.table import Table
from rich.theme import Theme


# noinspection DuplicatedCode
def load(module_name) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        module_name,
        os.path.join(
            os.getenv("MAKE_LIBRARIES_PATH"),
            f"{module_name}.py",
        ),
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


logger = load("log").get_logger()

RESET = "\033[0m"
BOLD = "\033[1m"
RED = "\033[1;31m"
GREEN = "\033[1;32m"
YELLOW = "\033[1;33m"
CYAN = "\033[1;36m"
DIM = "\033[38;5;243m"
ORANGE = "\033[1;38;5;208m"
WHITE = "\033[1;37m"

RENDER_WIDTH = 1024

# macOS system library prefixes - any dylib under these paths is acceptable
MACOS_SYSTEM_PREFIXES = (
    "/usr/lib/",
    "/System/",
)

# macOS relative reference prefixes - these are acceptable internal references
MACOS_RELATIVE_PREFIXES = (
    "@executable_path",
    "@loader_path",
    "@rpath",
)

# Linux system shared libraries - these are always present on any Linux system
LINUX_SYSTEM_LIBS = {
    "ld-linux",
    "libc.so",
    "libcrypt.so",
    "libdl.so",
    "libgcc_s.so",
    "libm.so",
    "libnsl.so",
    "libpthread.so",
    "libresolv.so",
    "librt.so",
    "libstdc++.so",
    "libutil.so",
    "linux-gate",
    "linux-vdso",
}

# manylinux2014 (PEP 599) guaranteed libraries for x86_64.
# Only these dynamic dependencies are acceptable in cross-compiled.
# See: https://peps.python.org/pep-0599/#the-manylinux2014-policy
MANYLINUX2014_ALLOWED_LIBS = {
    "ld-linux-x86-64.so.2",
    "libc.so.6",
    "libcrypt.so.1",
    "libdl.so.2",
    "libgcc_s.so.1",
    "libm.so.6",
    "libnsl.so.1",
    "libpthread.so.0",
    "libresolv.so.2",
    "librt.so.1",
    "libstdc++.so.6",
    "libutil.so.1",
}

# Windows system DLLs (case-insensitive matching)
WINDOWS_SYSTEM_DLLS = {
    "advapi32.dll",
    "bcrypt.dll",
    "bcryptprimitives.dll",
    "crypt32.dll",
    "dbghelp.dll",
    "iphlpapi.dll",
    "kernel32.dll",
    "mswsock.dll",
    "ncrypt.dll",
    "ntdll.dll",
    "ole32.dll",
    "oleaut32.dll",
    "pdh.dll",
    "powrprof.dll",
    "psapi.dll",
    "python3.dll",
    "secur32.dll",
    "setupapi.dll",
    "shell32.dll",
    "user32.dll",
    "userenv.dll",
    "vcruntime140_1.dll",
    "vcruntime140.dll",
    "ws2_32.dll",
}

# Windows system DLL prefixes (case-insensitive)
WINDOWS_SYSTEM_PREFIXES = ("api-ms-win-",)


TD_CROSS = os.getenv("TD_CROSS", "") == "true"


class LibInfo:
    def __init__(self, name: str, is_static: bool, is_system: bool):
        self.name = name
        self.is_static = is_static
        self.is_system = is_system

    @property
    def is_ok(self) -> bool:
        if TD_CROSS:
            return self.is_static or self.name in MANYLINUX2014_ALLOWED_LIBS
        return self.is_static or self.is_system


MACHO_MAGICS = {
    b"\xfe\xed\xfa\xce",
    b"\xfe\xed\xfa\xcf",
    b"\xce\xfa\xed\xfe",
    b"\xcf\xfa\xed\xfe",
    b"\xca\xfe\xba\xbe",
    b"\xbe\xba\xfe\xca",
}


def detect_file_type_by_magic(filepath: str) -> str | None:
    try:
        with open(filepath, "rb") as handle:
            magic = handle.read(4)
    except OSError:
        return None
    if magic[:2] == b"MZ":
        return "pe"
    if magic == b"\x7fELF":
        return "elf"
    if magic in MACHO_MAGICS:
        return "macho"
    return None


def detect_file_type(filepath: str) -> str | None:
    try:
        result = subprocess.run(
            [
                "file",
                filepath,
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        output = result.stdout
        if "Mach-O" in output:
            return "macho"
        if "ELF" in output:
            return "elf"
        if "PE32" in output:
            return "pe"
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return detect_file_type_by_magic(filepath)
    return None


def check_macho(filepath: str) -> list[LibInfo]:
    libs = []
    try:
        result = subprocess.run(
            [
                "otool",
                "-L",
                filepath,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        for line in result.stdout.strip().splitlines()[
            1:
        ]:  # skip first line (binary path)
            line = line.strip()
            if not line:
                continue
            # Format: "/path/to/lib.dylib (compatibility version X, current version Y)"
            lib_path = line.split(" (")[0].strip()
            if not lib_path:
                continue

            is_system = any(
                lib_path.startswith(p) for p in MACOS_SYSTEM_PREFIXES
            ) or any(lib_path.startswith(p) for p in MACOS_RELATIVE_PREFIXES)
            libs.append(LibInfo(lib_path, is_static=False, is_system=is_system))
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return libs


def check_elf(filepath: str) -> list[LibInfo]:
    libs = []
    try:
        result = subprocess.run(
            [
                "readelf",
                "-d",
                filepath,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        for line in result.stdout.splitlines():
            if "(NEEDED)" not in line:
                continue
            # Format: "  0x0000000000000001 (NEEDED)  Shared library: [libc.so.6]"
            start = line.find("[")
            end = line.find("]")
            if start == -1 or end == -1:
                continue
            lib_name = line[start + 1 : end]
            is_system = any(lib_name.startswith(s) for s in LINUX_SYSTEM_LIBS)
            libs.append(LibInfo(lib_name, is_static=False, is_system=is_system))
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return libs


def check_pe(filepath: str) -> list[LibInfo]:
    libs = []
    dll_names = set()

    # Try dumpbin first (MSVC)
    try:
        result = subprocess.run(
            [
                "dumpbin",
                "/dependents",
                filepath,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            in_deps = False
            for line in result.stdout.splitlines():
                stripped = line.strip()
                if "Image has the following dependencies" in line:
                    in_deps = True
                    continue
                if in_deps and stripped.lower().endswith(".dll"):
                    dll_names.add(stripped)
                elif in_deps and stripped == "":
                    in_deps = False
    except FileNotFoundError:
        pass

    # Fallback to objdump (MinGW)
    if not dll_names:
        try:
            result = subprocess.run(
                [
                    "objdump",
                    "-p",
                    filepath,
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            for line in result.stdout.splitlines():
                if "DLL Name:" in line:
                    dll_name = line.split("DLL Name:")[-1].strip()
                    if dll_name:
                        dll_names.add(dll_name)
        except FileNotFoundError:
            pass

    for dll in sorted(dll_names):
        dll_lower = dll.lower()
        is_system = dll_lower in WINDOWS_SYSTEM_DLLS or any(
            dll_lower.startswith(p) for p in WINDOWS_SYSTEM_PREFIXES
        )
        libs.append(LibInfo(dll, is_static=False, is_system=is_system))

    return libs


def get_libs(filepath: str) -> list[LibInfo] | None:
    if not os.path.isfile(filepath):
        logger.warning(f"File not found: {filepath}")
        return None

    file_type = detect_file_type(filepath)
    if file_type is None:
        return None

    if file_type == "macho":
        return check_macho(filepath)
    elif file_type == "elf":
        return check_elf(filepath)
    elif file_type == "pe":
        return check_pe(filepath)
    return None


def print_report(results: list[tuple[str, list[LibInfo]]]) -> bool:
    all_ok = True

    console = Console(
        width=RENDER_WIDTH,
        theme=Theme({
            "title": "bold bright_cyan",
            "binary": "bold bright_cyan",
            "binary.fail": "bold bright_red",
            "lib.system": "dim",
            "lib.reject": "bold bright_red",
            "link.dynamic": "yellow",
            "link.static": "bold bright_green",
            "type.system": "dim",
            "type.external": "bold rgb(255,165,0)",
            "status.ok": "dim",
            "status.binary.ok": "bold bright_green",
            "status.reject": "bold bright_red",
            "header": "bold bright_white",
        }),
    )

    table = Table(
        title="Native Libraries Check",
        title_style="title",
        show_header=True,
        header_style="header",
        box=box.ROUNDED,
        pad_edge=True,
        expand=False,
        padding=(0, 1),
        border_style="bright_blue",
    )
    table.add_column("Library", no_wrap=True)
    table.add_column("Linking", no_wrap=True)
    table.add_column("Type", no_wrap=True)
    table.add_column("Status", no_wrap=True)

    first = True
    for filepath, libs in results:
        binary_name = os.path.basename(filepath)
        binary_ok = all(lib.is_ok for lib in libs)
        if not binary_ok:
            all_ok = False

        if not first:
            table.add_section()
        first = False

        status_text = (
            "[status.binary.ok]success[/]" if binary_ok else "[status.reject]failure[/]"
        )
        binary_style = "binary" if binary_ok else "binary.fail"
        table.add_row(
            f"[{binary_style}]{binary_name}[/]",
            "",
            "",
            status_text,
        )

        if not libs:
            table.add_row(
                "[lib.system]  (statically linked)[/]",
                "",
                "",
                "[status.ok]ok[/]",
            )
        else:
            for lib in sorted(libs, key=lambda entry: entry.name.lower()):
                if lib.is_ok:
                    name_str = f"[lib.system]  {lib.name}[/]"
                else:
                    name_str = f"[lib.reject]  {lib.name}[/]"

                if lib.is_static:
                    link_str = "[link.static]static[/]"
                else:
                    link_str = "[link.dynamic]dynamic[/]"

                if lib.is_system:
                    type_str = "[type.system]system[/]"
                else:
                    type_str = "[type.external]external[/]"

                if lib.is_ok:
                    status_str = "[status.ok]accepted[/]"
                else:
                    status_str = "[status.reject]rejected[/]"

                table.add_row(name_str, link_str, type_str, status_str)

    console.print()
    console.print(table)

    return all_ok


def find_binaries_from_cargo(manifest_path: str, target_folder: str) -> list[str]:
    manifest_path = os.path.abspath(manifest_path)
    target_folder = os.path.abspath(target_folder)
    is_windows = platform.system() == "Windows"

    try:
        result = subprocess.run(
            [
                "cargo",
                "metadata",
                "--manifest-path",
                manifest_path,
                "--format-version",
                "1",
                "--no-deps",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            logger.error(f"cargo metadata failed: {result.stderr.strip()}")
            return []
        metadata = json.loads(result.stdout)
    except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"cargo metadata error: {e}")
        return []

    binaries = []
    for package in metadata.get("packages", []):
        for target in package.get("targets", []):
            kinds = target.get("kind", [])
            name = target.get("name", "")
            if not name:
                continue

            if "bin" in kinds:
                ext = ".exe" if is_windows else ""
                bin_path = os.path.join(target_folder, f"{name}{ext}")
            elif "cdylib" in kinds:
                if is_windows:
                    bin_path = os.path.join(target_folder, f"{name}.dll")
                elif platform.system() == "Darwin":
                    bin_path = os.path.join(target_folder, f"lib{name}.dylib")
                else:
                    bin_path = os.path.join(target_folder, f"lib{name}.so")
            else:
                continue

            if os.path.isfile(bin_path):
                binaries.append(bin_path)
            else:
                logger.debug(f"Target not found (skipping): {bin_path}")

    return binaries


def run_check(files: list[str]) -> None:
    results: list[tuple[str, list[LibInfo]]] = []
    for filepath in sorted(files):
        filepath = os.path.abspath(filepath)
        libs = get_libs(filepath)
        if libs is None:
            continue
        results.append((filepath, libs))

    if not results:
        logger.info("No native binaries to check.")
        return

    all_ok = print_report(results)

    print()
    if all_ok:
        print(f"  {GREEN}All binaries passed native libraries check.{RESET}")
    else:
        print(
            f"  {RED}Failed: some binaries have unacceptable "
            f"dynamic dependencies.{RESET}"
        )
        print(
            f"  {ORANGE}All external libraries must be statically linked "
            f"to avoid runtime failures on target machines.{RESET}"
        )
    print()

    sys.exit(0 if all_ok else 1)


def main():
    parser = argparse.ArgumentParser(
        description="Check native libraries for non-system dynamic dependencies",
    )
    subparsers = parser.add_subparsers(dest="command")

    files_parser = subparsers.add_parser(
        "files",
        help="Check specific binary files",
    )
    files_parser.add_argument(
        "paths",
        nargs="+",
        type=str,
        help="Path(s) to native binary files to check",
    )

    cargo_parser = subparsers.add_parser(
        "cargo",
        help="Discover and check binaries from a Cargo.toml workspace",
    )
    cargo_parser.add_argument(
        "manifest_path",
        type=str,
        help="Path to the workspace Cargo.toml",
    )
    cargo_parser.add_argument(
        "target_folder",
        type=str,
        help="Path to the target build folder (e.g. target/debug)",
    )

    args = parser.parse_args()

    if args.command == "files":
        run_check(args.paths)
    elif args.command == "cargo":
        binaries = find_binaries_from_cargo(args.manifest_path, args.target_folder)
        if not binaries:
            logger.info(f"No binaries found in {args.target_folder}")
            return
        run_check(binaries)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
