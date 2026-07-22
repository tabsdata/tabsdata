#
# Copyright 2025 Tabsdata Inc.
#

# This is a failing import statement intentionally placed for a test
# noinspection PyUnresolvedReferences
from doesntexist import doesntexist

doesntexist()
raise ValueError
