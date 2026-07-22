#
#  Copyright 2026 Tabsdata Inc.
#

from importlib.resources import files
from importlib.resources.abc import Traversable


def resource(name: str) -> Traversable:
    return files(__package__).joinpath(name)
