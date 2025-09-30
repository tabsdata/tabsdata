#
# Copyright 2024 Tabs Data Inc.
#

import re
from typing import List

from tabsdata.exceptions import ErrorCode, TableURIConfigurationError

URI_SEPARATOR = "/"


class Version:
    """
    Version class to represent a Tabs Data version. The version is represented as a
        string. The version can be HEAD, HEAD^, HEAD^^, HEAD~1, HEAD~2, etc. or a
        26-character hexadecimal string (the hash of a specific commit).

    Attributes:
        version (str): The version of the URI.

    Methods:
        to_string() -> str: Return the version as a string.
    """

    VERSION_PATTERN = re.compile(
        r"^(HEAD\^*|HEAD~[0-9]+|INITIAL\^*|INITIAL~[0-9]+|[A-Z0-9]{26})$"
    )

    def __init__(self, version: str):
        """
        Initialize the Version object.

        Args:
            version (str): The version of the URI.
        """
        self.version = version

    @property
    def version(self) -> str:
        """
        str: The version of the URI.
        """
        return self._version

    @version.setter
    def version(self, version: str):
        """
        Set the version of the URI.

        Args:
            version (str): The version of the URI.
        """
        if isinstance(version, str):
            if self.VERSION_PATTERN.match(version):
                self._version = version
            else:
                raise TableURIConfigurationError(
                    ErrorCode.TUCE9, self.VERSION_PATTERN, version
                )
        else:
            raise TableURIConfigurationError(ErrorCode.TUCE1, type(version))

    def to_string(self) -> str:
        """
        Return the version as a string.
        """
        return self.version

    def __eq__(self, other) -> bool:
        if not isinstance(other, Version):
            return False
        return self.to_string() == other.to_string()

    def __str__(self) -> str:
        return self.to_string()


class VersionList:
    """
    VersionList class to represent a list of Tabs Data versions. The version list is
        represented as a list of Version objects.

    Attributes:
        version_list (List[Version]): The list of versions of the URI.

    Methods:
        to_string() -> str: Return the version list as a string.
    """

    def __init__(self, version_list: List[Version] | List[str]):
        """
        Initialize the VersionList object.

        Args:
            version_list (List[Version] | List[str]): The list of versions of the URI.
        """
        self.version_list = version_list

    @property
    def version_list(self) -> List[Version]:
        """
        List[Version]: The list of versions of the URI.
        """
        return self._version_list

    @version_list.setter
    def version_list(self, version_list: List[str] | List[Version]):
        """
        Set the list of versions of the URI.

        Args:
            version_list (List[str] | List[Version]): The list of versions of the URI.
        """
        if isinstance(version_list, list):
            if len(version_list) > 1:
                self._version_list = [
                    build_version_object(version) for version in version_list
                ]
            else:
                raise TableURIConfigurationError(
                    ErrorCode.TUCE8, version_list, len(version_list)
                )
        else:
            raise TableURIConfigurationError(ErrorCode.TUCE6, list, type(version_list))

        for version in self._version_list:
            if not isinstance(version, Version):
                raise TableURIConfigurationError(
                    ErrorCode.TUCE7, Version, version, type(version)
                )

    def to_string(self) -> str:
        """
        Return the version list as a string.

        Returns:
            str: The version list as a string.
        """
        return ",".join([version.to_string() for version in self.version_list])

    def __eq__(self, other) -> bool:
        if not isinstance(other, VersionList):
            return False
        return self.to_string() == other.to_string()

    def __str__(self) -> str:
        return self.to_string()


class VersionRange:
    """
    VersionRange class to represent a range of Tabs Data versions. The version range is
        represented as two Version objects, indicating the beginning and ending of
        the range.

    Attributes:
        initial_version (Version): The initial version of the range.
        final_version (Version): The final version of the range.

    Methods:
        to_string() -> str: Return the version range as a string.
    """

    def __init__(self, initial_version: str | Version, final_version: str | Version):
        """
        Initialize the VersionRange object.

        Args:
            initial_version (str | Version): The initial version of the range.
            final_version (str | Version): The final version of the range.
        """
        self.initial_version = initial_version
        self.final_version = final_version

    @property
    def initial_version(self) -> Version:
        """
        Version: The initial version of the range.
        """
        return self._initial_version

    @initial_version.setter
    def initial_version(self, initial_version: str | Version):
        """
        Set the initial version of the range.

        Args:
            initial_version (str | Version): The initial version of the range.
        """
        built_initial_version = build_version_object(initial_version)
        if isinstance(built_initial_version, Version):
            self._initial_version = built_initial_version
        else:
            raise TableURIConfigurationError(
                ErrorCode.TUCE2,
                Version,
                built_initial_version,
                type(built_initial_version),
            )

    @property
    def final_version(self) -> Version:
        """
        Version: The final version of the range.
        """
        return self._final_version

    @final_version.setter
    def final_version(self, final_version: str | Version):
        """
        Set the final version of the range.

        Args:
            final_version (str | Version): The final version of the range.
        """
        built_final_version = build_version_object(final_version)
        if isinstance(built_final_version, Version):
            self._final_version = built_final_version
        else:
            raise TableURIConfigurationError(
                ErrorCode.TUCE3, Version, built_final_version, type(built_final_version)
            )

    def to_string(self) -> str:
        """
        Return the version range as a string.

        Returns:
            str: The version range as a string.
        """
        return self.initial_version.to_string() + ".." + self.final_version.to_string()

    def __eq__(self, other) -> bool:
        if not isinstance(other, VersionRange):
            return False
        return self.to_string() == other.to_string()

    def __str__(self) -> str:
        return self.to_string()


def build_version_object(version: str | Version | VersionList | VersionRange):
    if isinstance(version, (Version, VersionRange, VersionList)):
        return version
    elif isinstance(version, str):
        if ".." in version:
            split_range = version.split("..")
            if len(split_range) == 2:
                return VersionRange(split_range[0], split_range[1])
            else:
                raise TableURIConfigurationError(ErrorCode.TUCE5, version)
        elif "," in version:
            split_list = version.split(",")
            return VersionList(split_list)
        else:
            return Version(version)
    else:
        raise TableURIConfigurationError(
            ErrorCode.TUCE4, [str, Version, VersionList, VersionRange], type(version)
        )


class TableURI:
    """
    URI class to represent a Tabs Data URI. The URI is composed of a collection, a
        table and a version. The URI is represented as
        collection/table@version or table@version. The
        collection and table are optional, but at least one of them must be
        present. The version is optional. The collection and table must be
        strings. The version can be a string, a Version object, a VersionList object
        or a VersionRange object.

    Attributes:
        collection (str): The collection of the URI.
        table (str): The table of the URI.
        version (Version | VersionList | VersionRange | None): The version of the URI.

    Methods:
        to_string() -> str: Return the URI as a string.
    """

    def __init__(
        self,
        collection: str | None = None,
        table: str = None,
        version: str | Version | VersionList | VersionRange | None = None,
    ):
        """
        Initialize the URI object.

        Args:
            collection (str | None): The collection of the URI.
            table (str | None): The table of the URI.
            version (str | Version | VersionList | VersionRange | None): The version of
                the URI. If it is a string, it can be a single version, a list of
                versions separated by commas or a range of versions separated by two
                dots. If it is a Version, VersionList or VersionRange object, it will be
                used as is.
        """
        self._fully_built = False
        self.collection = collection
        self.table = table
        self.version = version
        self._verify_valid_uri()
        self._fully_built = True

    @property
    def collection(self) -> str:
        """
        str: The collection of the URI.
        """
        return self._collection

    @collection.setter
    def collection(self, collection: str | None):
        """
        Set the collection of the URI.

        Args:
            collection (str | None): The collection of the URI.
        """
        if collection is None:
            self._collection = ""
        elif isinstance(collection, str):
            self._collection = collection
        else:
            raise TableURIConfigurationError(ErrorCode.TUCE10, type(collection))
        if self._fully_built:
            self._verify_valid_uri()

    @property
    def table(self) -> str:
        """
        str: The table of the URI.
        """
        return self._table

    @table.setter
    def table(self, table: str | None):
        """
        Set the table of the URI.

        Args:
            table (str | None): The table of the URI.
        """
        if table is None:
            self._table = ""
        elif isinstance(table, str):
            self._table = table
        else:
            raise TableURIConfigurationError(ErrorCode.TUCE12, type(table))
        if self._fully_built:
            self._verify_valid_uri()

    @property
    def version(self) -> Version | VersionList | VersionRange | None:
        """
        Version | VersionList | VersionRange | None: The version(s) of the URI.
        """
        return self._version

    @version.setter
    def version(self, version: str | Version | VersionList | VersionRange | None):
        """
        Set the version of the URI.

        Args:
            version (str | Version | VersionList | VersionRange | None): The
                version(s) of the URI. If it is a string, it can be a single version,
                a list of versions separated by commas or a range of versions separated
                by two dots. If it is a Version, VersionList or VersionRange object, it
                will be used as is.
        """
        if version is None:
            self._version = None
        else:
            self._version = build_version_object(version)
        if self._fully_built:
            self._verify_valid_uri()

    def to_string(self) -> str:
        """
        Return the URI as a string.

        Returns:
            str: The URI as a string.
        """
        uri = ""
        if self.collection:
            uri = f"{self.collection}"
            if self.table:
                uri += f"/{self.table}"
        elif self.table:
            uri += f"{self.table}"
        if self.version:
            uri += "@" + self.version.to_string()
        return uri

    def _verify_valid_uri(self):
        """
        Verify that the URI is valid. It must have at least a table.
        """
        if not self.table:
            raise TableURIConfigurationError(ErrorCode.TUCE15)

    def __eq__(self, other) -> bool:
        if not isinstance(other, TableURI):
            return False
        return self.to_string() == other.to_string()

    def __str__(self) -> str:
        return self.to_string()


def build_table_uri_object(uri: str | TableURI) -> TableURI:
    original_uri = uri
    if isinstance(uri, TableURI):
        return uri
    elif isinstance(uri, str):
        pattern = re.compile(
            r"^(?P<collection>[^/@]+/)?(?P<table>[^/@]+)(@(?P<version>[^/@]+))?$"
        )
        match = pattern.match(uri)
        if match:
            collection = match.group("collection")
            if collection:
                collection = collection[:-1]
            return TableURI(collection, match.group("table"), match.group("version"))
        else:
            raise TableURIConfigurationError(ErrorCode.TUCE13, original_uri)
    else:
        raise TableURIConfigurationError(ErrorCode.TUCE14, type(original_uri))
