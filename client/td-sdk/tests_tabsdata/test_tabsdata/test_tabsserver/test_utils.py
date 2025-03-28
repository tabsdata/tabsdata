#
# Copyright 2024 Tabs Data Inc.
#

import os
import pathlib

from tabsdata.tabsserver.utils import convert_uri_to_path


def test_convert_uri_to_path():
    # We use current directory instead of a hardcoded value so that it properly
    # tests this function in every os
    path = os.getcwd()
    uri = pathlib.Path(path).as_uri()
    assert convert_uri_to_path(uri) == path
