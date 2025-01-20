#
# Copyright 2024 Tabs Data Inc.
#

from abc import ABC, abstractmethod


class InterceptorTestPlugin(ABC):
    IDENTIFIER = "ta_interceptor_test"

    @abstractmethod
    def check_test_examples(self, folder):
        pass
