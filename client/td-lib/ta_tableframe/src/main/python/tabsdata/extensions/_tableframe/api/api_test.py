#
# Copyright 2024 Tabs Data Inc.
#

from abc import ABC, abstractmethod


class TestTableFrameExtension(ABC):
    IDENTIFIER = "ta_tableframe_test"

    @abstractmethod
    def check_test_examples(self, folder):
        pass
