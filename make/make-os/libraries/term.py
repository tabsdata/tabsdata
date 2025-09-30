#
#  Copyright 2025 Tabs Data Inc.
#

import os
import shutil


def cols():  # noqa: C901
    for fd in (0, 1, 2):
        try:
            return os.get_terminal_size(fd).columns
        except OSError:
            pass
    if os.name == "posix":
        # noinspection PyBroadException
        try:
            fd = os.open(os.ctermid(), os.O_RDONLY)
            try:
                return os.get_terminal_size(fd).columns
            finally:
                os.close(fd)
        except Exception:
            pass
    if os.name == "nt":
        # noinspection PyBroadException
        try:
            with open("CONOUT$", "w") as con:
                return os.get_terminal_size(con.fileno()).columns
        except Exception:
            pass
    return shutil.get_terminal_size().columns


print(min(cols(), 256))
