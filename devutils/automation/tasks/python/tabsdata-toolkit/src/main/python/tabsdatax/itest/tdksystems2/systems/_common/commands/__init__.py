#
# Copyright 2026 Tabsdata Inc.
#

"""Generic verb implementations shared by every system.

Each module here exposes the command contract (``add_arguments`` / ``run``) for
one verb, written against the backend Protocol (``ctx.system``) so it works for
any system. A concrete system gets these by default (the dispatcher resolves a
verb to the module here unless the system's own package overrides it) and only
writes its own command module when it needs different options or behavior.
"""
