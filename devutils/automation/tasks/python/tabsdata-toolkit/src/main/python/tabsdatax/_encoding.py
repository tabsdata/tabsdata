#
#  Copyright 2026 Tabsdata Inc.
#


def setup_utf8_streams():
    import sys

    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        encoding = (getattr(stream, "encoding", "") or "").lower().replace("-", "")
        if encoding == "utf8":
            continue
        # noinspection PyBroadException
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


setup_utf8_streams()
