#
# Copyright 2025 Tabs Data Inc.
#

from abc import abstractmethod


class Conn:
    """
    Represents a connection configuration. This is a base class.
    """

    @classmethod
    @abstractmethod
    def _load_from_dict(cls, configs: dict) -> "Conn":
        """
        Construct a Conn instance from a plain dictionary of connection catalog
        configurations.

        Args:
            configs: A dictionary of configuration values, with env vars already
                resolved.
        Returns:
            A Conn instance.
        """
        raise NotImplementedError
