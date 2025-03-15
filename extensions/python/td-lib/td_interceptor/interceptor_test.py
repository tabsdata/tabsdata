#
# Copyright 2024 Tabs Data Inc.
#

import os
from abc import ABC

from ta_interceptor.api.api_test import InterceptorTestPlugin

from td_interceptor.version import version


class InterceptorTest(InterceptorTestPlugin, ABC):
    name = "Interceptor Test Plugin (Standard)"
    version = version()

    @classmethod
    def instance(cls) -> "InterceptorTest":
        return instance

    def check_test_examples(self, folder):
        assert not os.path.exists(os.path.join(folder, "spanish.jsonl"))
        assert not os.path.exists(os.path.join(folder, "french.jsonl"))


instance = InterceptorTest()
