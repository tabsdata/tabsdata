#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import struct
import sys
from dataclasses import dataclass
from typing import Generic, Mapping, TextIO, TypeVar

from tabsdata.expansions.foundation.graphics import ImageProtocol, render

T = TypeVar("T")

TD_IMAGE_PROTOCOL_VARIABLE = "TD_IMAGE_PROTOCOL"
TERM_VARIABLE = "TERM"
TERM_PROGRAM_VARIABLE = "TERM_PROGRAM"
LC_TERMINAL_VARIABLE = "LC_TERMINAL"

PROTOCOLS = {
    "kitty": ImageProtocol.Kitty,
    "iterm2": ImageProtocol.Iterm2,
    "halfblocks": ImageProtocol.Halfblocks,
}

TEXT_ONLY_TERMS = ("dumb", "linux", "unknown")
MULTIPLEXER_VARIABLES = ("TMUX", "STY")
FOREIGN_CONSOLE_VARIABLES = (
    "TERMINAL_EMULATOR",
    "PYCHARM_HOSTED",
    "IDEA_INITIAL_DIRECTORY",
)
KITTY_TERMS = ("xterm-kitty", "xterm-ghostty")
KITTY_TERM_PROGRAMS = ("ghostty",)
ITERM2_TERM_PROGRAMS = ("iTerm.app", "WezTerm")
ITERM2_LC_TERMINALS = ("iTerm2",)


@dataclass(frozen=True)
class Figure(Generic[T]):
    text: T
    pixels: list[str] | None = None


def figure(
    image: str | os.PathLike[str] | None,
    text: T,
    width: int,
    columns: int,
    stream: TextIO | None = None,
    environ: Mapping[str, str] | None = None,
) -> Figure[T]:
    stream = sys.stdout if stream is None else stream
    environ = os.environ if environ is None else environ
    if image is None:
        return Figure(text)
    protocol = image_protocol(stream, environ)
    if protocol is None:
        return Figure(text)
    columns = max(1, min(columns, width))
    try:
        rows = render(os.fspath(image), columns, protocol, cell_size(stream))
    except Exception:
        return Figure(text)
    indent = " " * max(0, (width - columns) // 2)
    return Figure(text, [indent + row for row in rows])


def image_protocol(stream: TextIO, environ: Mapping[str, str]) -> ImageProtocol | None:
    forced = environ.get(TD_IMAGE_PROTOCOL_VARIABLE, "").strip().lower()
    if forced:
        return PROTOCOLS.get(forced)
    if not is_terminal(stream):
        return None
    term = environ.get(TERM_VARIABLE, "")
    term_program = environ.get(TERM_PROGRAM_VARIABLE, "")
    if term in TEXT_ONLY_TERMS:
        return None
    if any(environ.get(variable) for variable in MULTIPLEXER_VARIABLES):
        return None
    if any(environ.get(variable) for variable in FOREIGN_CONSOLE_VARIABLES):
        return None
    if term in KITTY_TERMS or term_program in KITTY_TERM_PROGRAMS:
        return ImageProtocol.Kitty
    if term_program in ITERM2_TERM_PROGRAMS:
        return ImageProtocol.Iterm2
    if not term_program and environ.get(LC_TERMINAL_VARIABLE) in ITERM2_LC_TERMINALS:
        return ImageProtocol.Iterm2
    return None


def is_terminal(stream: TextIO) -> bool:
    try:
        return stream.isatty()
    except (AttributeError, ValueError, OSError):
        return False


def cell_size(stream: TextIO) -> tuple[int, int] | None:
    try:
        import fcntl
        import termios

        packed = fcntl.ioctl(stream.fileno(), termios.TIOCGWINSZ, bytes(8))
    except Exception:
        return None
    rows, columns, width, height = struct.unpack("HHHH", packed)
    if not (rows and columns and width and height):
        return None
    return width // columns, height // rows
