#
# Copyright 2024 Tabs Data Inc.
#

import os
import re
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any

import hvac

from tabsdata.exceptions import ErrorCode, SecretConfigurationError


class SecretIdentifier(Enum):
    DIRECT_SECRET = "direct-secret"
    ENVIRONMENT_SECRET = "environment-secret"
    HASHICORP_SECRET = "hashicorp-secret"


class Secret(ABC):
    """Secrets class to store the credentials needed to access different
    services."""

    @abstractmethod
    def _to_dict(self) -> dict:
        """Convert the secret object to a dictionary."""

    @property
    @abstractmethod
    def secret_value(self) -> str:
        """Get the secret value pointed at by the secret. To be used only during
        execution in the backend."""

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Secret):
            return False
        return self._to_dict() == other._to_dict()

    def __repr__(self) -> str:
        """
        Returns a string representation of the DirectSecret.

        Returns:
            str: A string representation of the DirectSecret.
        """
        return f"{self.__class__.__name__}({self._to_dict()[self.IDENTIFIER]})"


class HashiCorpSecret(Secret):
    """Secrets class representing a secret stored in Hashicorp Vault.

    Attributes:
        path (str): The path to the secret in Hashicorp Vault.
        name (str): The name of the secret in Hashicorp Vault.
        vault (str): If multiple vaults exist in the system, the name of the vault to
            use. When executing in the server, the URL and token associated to that
            specific vault will be used. The name can only contain uppercase letters,
            numbers and underscores, and can't begin with a number. Defaults to
            "HASHICORP".

    Methods:
        to_dict() -> dict: Convert the HashiCorpSecret object to a dictionary.
        secret_value() -> str: Get the secret value.
    """

    IDENTIFIER = SecretIdentifier.HASHICORP_SECRET.value

    PATH_KEY = "path"
    NAME_KEY = "name"
    VAULT_KEY = "vault"

    VAULT_NAMESPACE_ENV_VAR = "TDS_HASHICORP_NAMESPACE"
    VAULT_TOKEN_ENV_VAR = "TDS_HASHICORP_TOKEN"
    VAULT_URL_ENV_VAR = "TDS_HASHICORP_URL"

    def __init__(self, path: str, name: str, vault: str = "HASHICORP"):
        """
        Initialize the HashiCorpSecret object.

        Args:
            path (str): The path to the secret in Hashicorp Vault.
            name (str): The name of the secret in Hashicorp Vault.
            vault (str, optional): If multiple vaults exist in the system, the name
                of the vault to use. When executing in the server, the URL and token
                associated to that specific vault will be used. The name can only
                contain uppercase letters, numbers and underscores, and can't begin
                with a number. Defaults to "HASHICORP".
        """
        self.path = path
        self.name = name
        self.vault = vault

    @property
    def vault(self) -> str:
        return self._vault

    @vault.setter
    def vault(self, vault: str):
        if not isinstance(vault, str):
            raise SecretConfigurationError(ErrorCode.SCE4, type(vault))
        pattern = r"^[A-Z_][A-Z0-9_]*$"
        is_valid = bool(re.match(pattern, vault))
        if not is_valid:
            raise SecretConfigurationError(ErrorCode.SCE5, vault)
        self._vault = vault

    def _to_dict(self) -> dict:
        """
        Convert the HashiCorpSecret object to a dictionary.

        Returns:
            dict: A dictionary representation of the HashiCorpSecret object.
        """
        return {
            self.IDENTIFIER: {
                self.PATH_KEY: self.path,
                self.NAME_KEY: self.name,
                self.VAULT_KEY: self.vault,
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
        vault_url_env_var = (
            self.VAULT_URL_ENV_VAR.replace("HASHICORP", self.vault, 1)
            if self.vault
            else self.VAULT_URL_ENV_VAR
        )
        try:
            vault_url = os.environ[vault_url_env_var]
        except KeyError:
            raise ValueError(f"Environment variable {vault_url_env_var} not found.")
        vault_token_env_var = (
            self.VAULT_TOKEN_ENV_VAR.replace("HASHICORP", self.vault, 1)
            if self.vault
            else self.VAULT_TOKEN_ENV_VAR
        )
        try:
            vault_token = os.environ[vault_token_env_var]
        except KeyError:
            raise ValueError(f"Environment variable {vault_token_env_var} not found.")
        try:
            namespace_env_var = (
                self.VAULT_NAMESPACE_ENV_VAR.replace("HASHICORP", self.vault, 1)
                if self.vault
                else self.VAULT_NAMESPACE_ENV_VAR
            )
            namespace = os.environ.get(namespace_env_var)
            client = hvac.Client(url=vault_url, token=vault_token, namespace=namespace)
            secret = client.secrets.kv.read_secret_version(
                self.path, raise_on_deleted_version=False
            )
            return secret["data"]["data"][self.name]
        except Exception:
            raise ValueError(
                "Error while retrieving secret from Hashicorp Vault. "
                "Please verify the secret path and name, as well as the "
                "environment variables for the URL and the token (and the namespace "
                "if using one)."
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

    def _to_dict(self) -> dict:
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

    def _to_dict(self) -> dict:
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
            ErrorCode.SCE3, [str, Secret], type(configuration)
        )


def _recursively_load_secret(value: Any) -> Any:
    if isinstance(value, dict):
        valid_identifiers = [element.value for element in SecretIdentifier]
        if len(value) == 1 and list(value.keys())[0] in valid_identifiers:
            return build_secret(value)
        return {key: _recursively_load_secret(val) for key, val in value.items()}
    elif isinstance(value, list):
        return [_recursively_load_secret(val) for val in value]
    elif isinstance(value, tuple):
        return tuple(_recursively_load_secret(val) for val in value)
    else:
        return value


def _recursively_evaluate_secret(value: Any) -> Any:
    if isinstance(value, dict):
        valid_identifiers = [element.value for element in SecretIdentifier]
        if len(value) == 1 and list(value.keys())[0] in valid_identifiers:
            return build_secret(value).secret_value
        return {
            _recursively_evaluate_secret(key): _recursively_evaluate_secret(val)
            for key, val in value.items()
        }
    elif isinstance(value, Secret):
        return value.secret_value
    elif isinstance(value, list):
        return [_recursively_evaluate_secret(val) for val in value]
    elif isinstance(value, tuple):
        return tuple(_recursively_evaluate_secret(val) for val in value)
    else:
        return value
