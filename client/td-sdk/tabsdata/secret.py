#
# Copyright 2024 Tabs Data Inc.
#

import os
from abc import ABC, abstractmethod
from enum import Enum

from tabsdata.exceptions import ErrorCode, SecretConfigurationError


class SecretIdentifier(Enum):
    DIRECT_SECRET = "direct-secret"
    ENVIRONMENT_SECRET = "environment-secret"
    HASHICORP_SECRET = "hashicorp-secret"


class Secret(ABC):
    """Secrets class to store the credentials needed to access different
    services."""

    @abstractmethod
    def to_dict(self) -> dict:
        """Convert the secret object to a dictionary."""

    @property
    @abstractmethod
    def secret_value(self) -> str:
        """Get the secret value pointed at by the secret. To be used only during
        execution in the backend."""

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Secret):
            return False
        return self.to_dict() == other.to_dict()

    def __repr__(self) -> str:
        """
        Returns a string representation of the DirectSecret.

        Returns:
            str: A string representation of the DirectSecret.
        """
        return f"{self.__class__.__name__}({self.to_dict()[self.IDENTIFIER]})"


# TODO: Implement the actual functionality of the HashiCorpSecret class once the backend
#  implementation is ready. https://tabsdata.atlassian.net/browse/TAB-95
class HashiCorpSecret(Secret):
    """Secrets class representing a secret stored in Hashicorp Vault.

    Attributes:
        secret_name (str): The name of the secret in Hashicorp Vault.

    Methods:
        to_dict() -> dict: Convert the HashiCorpSecret object to a dictionary.
        secret_value() -> str: Get the secret value.
    """

    IDENTIFIER = SecretIdentifier.HASHICORP_SECRET.value

    SECRET_NAME_KEY = "secret_name"

    def __init__(self, secret_name: str):
        """
        Initialize the HashiCorpSecret object.

        Args:
            secret_name (str): The name of the secret in Hashicorp Vault.
        """
        self.secret_name = secret_name

    def to_dict(self) -> dict:
        """
        Convert the HashiCorpSecret object to a dictionary.

        Returns:
            dict: A dictionary representation of the HashiCorpSecret object.
        """
        return {
            self.IDENTIFIER: {
                self.SECRET_NAME_KEY: self.secret_name,
            }
        }

    @property
    def secret_value(self) -> str:
        """
        Get the secret value pointed at by the secret. To be used only during execution
        in the backend.

        Returns:
            str: The secret value.
        """
        raise NotImplementedError(
            "This method is not implemented yet."
            " https://tabsdata.atlassian.net/browse/TAB-95"
        )


class DirectSecret(Secret):
    """Secrets class representing a secret stored in plain text.

    Attributes:
        secret_direct_value (str): The secret value.

    Methods:
        to_dict() -> dict: Convert the DirectSecret object to a dictionary.
        secret_value() -> str: Get the secret value.
    """

    IDENTIFIER = SecretIdentifier.DIRECT_SECRET.value

    SECRET_DIRECT_VALUE_KEY = "secret_direct_value"

    def __init__(self, secret_direct_value: str):
        """
        Initialize the DirectSecret object.

        Args:
            secret_direct_value (str): The secret value.
        """
        self.secret_direct_value = secret_direct_value

    def to_dict(self) -> dict:
        """
        Convert the DirectSecret object to a dictionary.

        Returns:
            dict: A dictionary representation of the DirectSecret object.
        """
        return {
            self.IDENTIFIER: {
                self.SECRET_DIRECT_VALUE_KEY: self.secret_direct_value,
            }
        }

    @property
    def secret_value(self) -> str:
        """
        Get the secret value pointed at by the secret. To be used only during execution
        in the backend.

        Returns:
            str: The secret value.
        """
        return self.secret_direct_value


class EnvironmentSecret(Secret):
    """Secrets class representing a secret obtained from an environment variable in
        the server.

    Attributes:
        environment_variable_name (str): Name of the environment variable from which
            we will obtain the secret value.

    Methods:
        to_dict() -> dict: Convert the EnvironmentSecret object to a dictionary.
        secret_value() -> str: Get the secret value.
    """

    IDENTIFIER = SecretIdentifier.ENVIRONMENT_SECRET.value

    ENVIRONMENT_VARIABLE_NAME_KEY = "environment_variable_name"

    def __init__(self, environment_variable_name: str):
        """
        Initialize the EnvironmentSecret object.

        Args:
            environment_variable_name (str): Name of the environment variable from which
            we will obtain the secret value.
        """
        self.environment_variable_name = environment_variable_name

    def to_dict(self) -> dict:
        """
        Convert the EnvironmentSecret object to a dictionary.

        Returns:
            dict: A dictionary representation of the EnvironmentSecret object.
        """
        return {
            self.IDENTIFIER: {
                self.ENVIRONMENT_VARIABLE_NAME_KEY: self.environment_variable_name,
            }
        }

    @property
    def secret_value(self) -> str:
        """
        Get the secret value pointed at by the secret. To be used only during execution
        in the backend.

        Returns:
            str: The secret value.
        """
        secret = os.environ.get(self.environment_variable_name, None)
        if secret is None:
            raise ValueError(
                f"Environment variable {self.environment_variable_name} not found."
            )
        return secret


def build_secret(
    configuration: str | dict | Secret,
) -> Secret | DirectSecret | EnvironmentSecret:
    """
    Builds a secret object from a dictionary, a string or a Secret Object.

    Returns:
        Secret | DirectSecret | EnvironmentSecret: The secret object.
    """
    if isinstance(configuration, Secret):
        return configuration
    elif isinstance(configuration, str):
        return DirectSecret(secret_direct_value=configuration)
    elif isinstance(configuration, dict):
        valid_identifiers = [element.value for element in SecretIdentifier]
        # The input dictionary must have exactly one key, which must be one of the
        # valid identifiers
        if (
            len(configuration) != 1
            or next(iter(configuration)) not in valid_identifiers
        ):
            raise SecretConfigurationError(
                ErrorCode.SCE1, valid_identifiers, list(configuration.keys())
            )
        # Since we have only one key, we select the identifier and the configuration
        identifier, secret_configuration = next(iter(configuration.items()))
        # The configuration must be a dictionary
        if not isinstance(secret_configuration, dict):
            raise SecretConfigurationError(
                ErrorCode.SCE2, identifier, type(secret_configuration)
            )
        if identifier == SecretIdentifier.DIRECT_SECRET.value:
            return DirectSecret(**secret_configuration)
        elif identifier == SecretIdentifier.HASHICORP_SECRET.value:
            return HashiCorpSecret(**secret_configuration)
        elif identifier == SecretIdentifier.ENVIRONMENT_SECRET.value:
            return EnvironmentSecret(**secret_configuration)
    else:
        raise SecretConfigurationError(
            ErrorCode.SCE3, [dict, str, Secret], type(configuration)
        )
