#
#  Copyright 2026 Tabsdata Inc.
#

from importlib.resources import files
from importlib.resources.abc import Traversable


def image_resources(image: str) -> Traversable:
    return files(f"{__package__}.{image}")
