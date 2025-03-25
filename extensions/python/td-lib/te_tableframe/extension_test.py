#
# Copyright 2024 Tabs Data Inc.
#

import os
from abc import ABC

from ta_tableframe.api.api_test import TestTableFrameExtension
from te_tableframe.version import version


class Test(TestTableFrameExtension, ABC):
    name = "Test TableFrame Extension (Standard)"
    version = version()

    @classmethod
    def instance(cls) -> "Test":
        return instance

    def check_test_examples(self, folder):
        assert not os.path.exists(os.path.join(folder, "spanish.jsonl"))
        assert not os.path.exists(os.path.join(folder, "french.jsonl"))


instance = Test()
