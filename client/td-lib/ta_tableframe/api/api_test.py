#
# Copyright 2024 Tabs Data Inc.
#

from abc import ABC, abstractmethod


class TestTableframeExtension(ABC):
    IDENTIFIER = "ta_tableframe_test"

    @abstractmethod
    def check_test_examples(self, folder):
        pass
