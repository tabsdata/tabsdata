#
# Copyright 2026 Tabsdata Inc.
#

"""Import the functions a connector's tabsdata-instance tests register from a
checked-in project directory, the way a user's own project is laid out: plain
top-level modules (``publisher.py`` / ``subscriber.py``) under a directory that is
handed to ``register_function`` as the ``project_root``.

The directory has to be importable as that root -- the registered descriptor
stores the function as ``"<module>:<qualname>"`` and the runner re-imports it by
that name against the bundled tree -- so the module has to be imported top-level
here too, not as part of the surrounding test package.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def function_module(project_root: Path, name: str) -> ModuleType:
    """The `name` module of the function project at `project_root`.

    Every connector names its modules the same (`publisher` / `subscriber`), so the
    file is executed afresh on each call rather than served from ``sys.modules``:
    a session collecting two connectors would otherwise hand the second one the
    first one's functions. The module is still published under the bare name while
    it executes, so a project module importing a sibling (``subscriber`` importing
    ``publisher``) binds to this project's copy, and ``project_root`` goes on
    ``sys.path`` for the same reason.
    """
    root = str(project_root)
    if root in sys.path:
        sys.path.remove(root)
    sys.path.insert(0, root)
    spec = importlib.util.spec_from_file_location(name, project_root / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module
