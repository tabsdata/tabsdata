#
# Copyright 2024 Tabs Data Inc.
#

from abc import ABC, abstractmethod
from enum import Enum

from tabsdata.exceptions import CredentialsConfigurationError, ErrorCode
from tabsdata.secret import Secret, build_secret


class CredentialIdentifier(Enum):
    AZURE_ACCOUNT_KEY_CREDENTIALS = "account_key-credentials"
    S3_ACCESS_KEY_CREDENTIALS = "s3_access_key-credentials"
    USER_PASSWORD_CREDENTIALS = "user_password-credentials"


class Credentials(ABC):
    """Credentials class to store the credentials needed to access different
    services."""

    @abstractmethod
    def to_dict(self) -> dict:
        """Convert the credentials object to a dictionary."""

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Credentials):
            return False
        return self.to_dict() == other.to_dict()


class AzureCredentials(Credentials, ABC):
    """Credentials class to store the credentials needed to access Azure."""


class AzureAccountKeyCredentials(AzureCredentials):
    """Credentials class to store the credentials needed to access an Azure
    using account key credentials (account name and account key).

    Attributes:
        account_name (Secret): The Azure account name.
        account_key (Secret): The Azure account key.

    Methods:
        to_dict() -> dict: Convert the AzureAccountNameKeyCredentials object to a
            dictionary
    """

    IDENTIFIER = CredentialIdentifier.AZURE_ACCOUNT_KEY_CREDENTIALS.value

    ACCOUNT_NAME_KEY = "account_name"
    ACCOUNT_KEY_KEY = "account_key"

    def __init__(
        self,
        account_name: str | dict | Secret,
        account_key: str | dict | Secret,
    ):
        """
        Initialize the AzureAccountNameKeyCredentials object.

        Args:
            account_name (str | dict | Secret): The Azure account name.
            account_key (str | dict | Secret): The Azure account key.
        """
        self.account_name = account_name
        self.account_key = account_key

    def to_dict(self) -> dict:
        """
        Convert the S3AccessKeyCredentials object to a dictionary.

        Returns:
            dict: A dictionary representation of the S3AccessKeyCredentials object.
        """
        return {
            self.IDENTIFIER: {
                self.ACCOUNT_NAME_KEY: self.account_name.to_dict(),
                self.ACCOUNT_KEY_KEY: self.account_key.to_dict(),
            }
        }

    @property
    def account_name(self) -> Secret:
        """
        Secret: The Azure account name.
        """
        return self._account_name

    @account_name.setter
    def account_name(self, account_name: str | dict | Secret):
        """
        Set the Azure account name.

        Args:
            account_name (str | dict | Secret): The Azure account name.
        """
        self._account_name = build_secret(account_name)

    @property
    def account_key(self) -> Secret:
        """
        Secret: The Azure account key.
        """
        return self._account_key

    @account_key.setter
    def account_key(self, account_key: str | dict | Secret):
        """
        Set the Azure account key.

        Args:
            account_key (str | dict | Secret): The Azure account key.
        """
        self._account_key = build_secret(account_key)

    def __repr__(self) -> str:
        """
        Returns a string representation of the S3AccessKeyCredentials.

        Returns:
            str: A string representation of the S3AccessKeyCredentials.
        """
        return f"{self.__class__.__name__}({self.to_dict()[self.IDENTIFIER]})"


class S3Credentials(Credentials, ABC):
    """Credentials class to store the credentials needed to access an S3 bucket."""


class S3AccessKeyCredentials(S3Credentials):
    """Credentials class to store the credentials needed to access an S3 bucket
    using access key credentials (access key id and secret access key).

    Attributes:
        aws_access_key_id (Secret): The AWS access key id.
        aws_secret_access_key (Secret): The AWS secret access key.

    Methods:
        to_dict() -> dict: Convert the S3AccessKeyCredentials object to a dictionary
    """

    IDENTIFIER = CredentialIdentifier.S3_ACCESS_KEY_CREDENTIALS.value

    AWS_ACCESS_KEY_ID_KEY = "aws_access_key_id"
    AWS_SECRET_ACCESS_KEY_KEY = "aws_secret_access_key"

    def __init__(
        self,
        aws_access_key_id: str | dict | Secret,
        aws_secret_access_key: str | dict | Secret,
    ):
        """
        Initialize the S3AccessKeyCredentials object.

        Args:
            aws_access_key_id (str | dict | Secret): The AWS access key id.
            aws_secret_access_key (str | dict | Secret): The AWS secret access key.
        """
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def to_dict(self) -> dict:
        """
        Convert the S3AccessKeyCredentials object to a dictionary.

        Returns:
            dict: A dictionary representation of the S3AccessKeyCredentials object.
        """
        return {
            self.IDENTIFIER: {
                self.AWS_ACCESS_KEY_ID_KEY: self.aws_access_key_id.to_dict(),
                self.AWS_SECRET_ACCESS_KEY_KEY: self.aws_secret_access_key.to_dict(),
            }
        }

    @property
    def aws_access_key_id(self) -> Secret:
        """
        Secret: The AWS access key id.
        """
        return self._aws_access_key_id

    @aws_access_key_id.setter
    def aws_access_key_id(self, aws_access_key_id: str | dict | Secret):
        """
        Set the AWS access key id.

        Args:
            aws_access_key_id (str | dict | Secret): The AWS access key id.
        """
        self._aws_access_key_id = build_secret(aws_access_key_id)

    @property
    def aws_secret_access_key(self) -> Secret:
        """
        Secret: The AWS secret access key.
        """
        return self._aws_secret_access_key

    @aws_secret_access_key.setter
    def aws_secret_access_key(self, aws_secret_access_key: str | dict | Secret):
        """
        Set the AWS secret access key.

        Args:
            aws_secret_access_key (str | dict | Secret): The AWS secret access key.
        """
        self._aws_secret_access_key = build_secret(aws_secret_access_key)

    def __repr__(self) -> str:
        """
        Returns a string representation of the S3AccessKeyCredentials.

        Returns:
            str: A string representation of the S3AccessKeyCredentials.
        """
        return f"{self.__class__.__name__}({self.to_dict()[self.IDENTIFIER]})"


class UserPasswordCredentials(Credentials):
    """Credentials class to store a user and password pair.

    Attributes:
        user (Secret): The user.
        password (Secret): The password.

    Methods:
        to_dict() -> dict: Convert the UserPasswordCredentials object to a dictionary.
    """

    IDENTIFIER = CredentialIdentifier.USER_PASSWORD_CREDENTIALS.value

    USER_KEY = "user"
    PASSWORD_KEY = "password"

    def __init__(
        self,
        user: str | dict | Secret,
        password: str | dict | Secret,
    ):
        """
        Initialize the UserPasswordCredentials object.

        Args:
            user (str | dict | Secret): The user.
            password (str | dict | Secret): The password
        """
        self.user = user
        self.password = password

    def to_dict(self) -> dict:
        """
        Convert the UserPasswordCredentials object to a dictionary.

        Returns:
            dict: A dictionary representation of the UserPasswordCredentials object.
        """
        return {
            self.IDENTIFIER: {
                self.USER_KEY: self.user.to_dict(),
                self.PASSWORD_KEY: self.password.to_dict(),
            }
        }

    @property
    def user(self) -> Secret:
        """
        Secret: The user.
        """
        return self._user

    @user.setter
    def user(self, user: str | dict | Secret):
        """
        Set the user.

        Args:
            user (str | dict | Secret): The user.
        """
        self._user = build_secret(user)

    @property
    def password(self) -> Secret:
        """
        Secret: The password.
        """
        return self._password

    @password.setter
    def password(self, password: str | dict | Secret):
        """
        Set the password.

        Args:
            password (str | dict | Secret): The password.
        """
        self._password = build_secret(password)

    def __repr__(self) -> str:
        """
        Returns a string representation of the UserPasswordCredentials.

        Returns:
            str: A string representation of the UserPasswordCredentials.
        """
        return f"{self.__class__.__name__}({self.to_dict()[self.IDENTIFIER]})"


def build_credentials(configuration: dict | Credentials) -> Credentials:
    """
    Builds a Credentials object from a dictionary or a Credentials object.
    :return: A Credentials object.
    """
    if isinstance(configuration, Credentials):
        return configuration
    elif isinstance(configuration, dict):
        valid_identifiers = [element.value for element in CredentialIdentifier]
        # The input dictionary must have exactly one key, which must be one of the
        # valid identifiers
        if (
            len(configuration) != 1
            or next(iter(configuration)) not in valid_identifiers
        ):
            raise CredentialsConfigurationError(
                ErrorCode.CCE1, valid_identifiers, list(configuration.keys())
            )
        # Since we have only one key, we select the identifier and the configuration
        identifier, credentials_configuration = next(iter(configuration.items()))
        # The configuration must be a dictionary
        if not isinstance(credentials_configuration, dict):
            raise CredentialsConfigurationError(
                ErrorCode.CCE2, identifier, type(credentials_configuration)
            )
        if identifier == CredentialIdentifier.S3_ACCESS_KEY_CREDENTIALS.value:
            return S3AccessKeyCredentials(**credentials_configuration)
        elif identifier == CredentialIdentifier.USER_PASSWORD_CREDENTIALS.value:
            return UserPasswordCredentials(**credentials_configuration)
        elif identifier == CredentialIdentifier.AZURE_ACCOUNT_KEY_CREDENTIALS.value:
            return AzureAccountKeyCredentials(**credentials_configuration)
    else:
        raise CredentialsConfigurationError(
            ErrorCode.CCE3, [dict, Credentials], type(configuration)
        )
