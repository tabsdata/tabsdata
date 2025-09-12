#
# Copyright 2024 Tabs Data Inc.
#

import pytest

from tabsdata._tableuri import (
    TableURI,
    Version,
    VersionList,
    VersionRange,
    build_table_uri_object,
    build_version_object,
)
from tabsdata.exceptions import ErrorCode, TableURIConfigurationError

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


def test_version_all_correct():
    version_string = "HEAD~1"
    version = Version(version_string)
    assert version.version == version_string
    assert version.to_string() == version_string
    assert build_version_object(version) == version
    assert build_version_object(version.to_string()) == version


def test_version_update():
    version_string = "HEAD~1"
    version = Version(version_string)
    assert version.version == version_string
    assert version.to_string() == version_string
    assert build_version_object(version) == version
    assert build_version_object(version.to_string()) == version
    second_version_string = "HEAD^^^"
    version.version = second_version_string
    assert version.version == second_version_string
    assert version.to_string() == second_version_string
    assert build_version_object(version) == version
    assert build_version_object(version.to_string()) == version


def test_wrong_type_version_raises_exception():
    version_int = 42
    with pytest.raises(TableURIConfigurationError) as e:
        Version(version_int)
    assert e.value.error_code == ErrorCode.TUCE1


def test_version_range_all_correct_string_parameters():
    initial_version = "HEAD~7"
    final_version = "HEAD~2"
    version_range = VersionRange(initial_version, final_version)
    assert version_range.initial_version == Version(initial_version)
    assert version_range.final_version == Version(final_version)
    assert build_version_object(version_range) == version_range
    assert build_version_object(version_range.to_string()) == version_range
    assert version_range.to_string() == initial_version + ".." + final_version


def test_version_range_all_correct_object_parameters():
    initial_version = Version("HEAD~7")
    final_version = Version("HEAD~2")
    version_range = VersionRange(initial_version, final_version)
    assert version_range.initial_version == initial_version
    assert version_range.final_version == final_version
    assert build_version_object(version_range) == version_range
    assert build_version_object(version_range.to_string()) == version_range
    assert (
        version_range.to_string()
        == initial_version.to_string() + ".." + final_version.to_string()
    )


def test_version_range_wrong_type_initial_version_raises_exception():
    final_version = "HEAD"
    with pytest.raises(TableURIConfigurationError) as e:
        VersionRange(42, final_version)
    assert e.value.error_code == ErrorCode.TUCE4
    with pytest.raises(TableURIConfigurationError) as e:
        VersionRange("HEAD~1..HEAD", final_version)
    assert e.value.error_code == ErrorCode.TUCE2


def test_version_range_wrong_type_final_version_raises_exception():
    initial_version = "HEAD~10"
    with pytest.raises(TableURIConfigurationError) as e:
        VersionRange(initial_version, 42)
    assert e.value.error_code == ErrorCode.TUCE4
    with pytest.raises(TableURIConfigurationError) as e:
        VersionRange(initial_version, "HEAD~1..HEAD")
    assert e.value.error_code == ErrorCode.TUCE3


def test_version_range_update():
    initial_version = "HEAD~7"
    final_version = "HEAD~2"
    version_range = VersionRange(initial_version, final_version)
    assert version_range.initial_version == Version(initial_version)
    assert version_range.final_version == Version(final_version)
    assert build_version_object(version_range) == version_range
    assert version_range.to_string() == initial_version + ".." + final_version
    new_initial_version = "HEAD^^^"
    new_final_version = "HEAD"
    version_range.initial_version = new_initial_version
    version_range.final_version = new_final_version
    assert version_range.initial_version == Version(new_initial_version)
    assert version_range.final_version == Version(new_final_version)
    assert build_version_object(version_range) == version_range
    assert version_range.to_string() == new_initial_version + ".." + new_final_version


def test_build_version_object_wrong_type_raises_exception():
    with pytest.raises(TableURIConfigurationError) as e:
        build_version_object(42)
    assert e.value.error_code == ErrorCode.TUCE4


def test_build_version_object_multiple_version_range_raises_exception():
    with pytest.raises(TableURIConfigurationError) as e:
        build_version_object("HEAD~2..HEAD~1..HEAD")
    assert e.value.error_code == ErrorCode.TUCE5


def test_build_version_object_wrong_range_raises_exception():
    with pytest.raises(TableURIConfigurationError) as e:
        build_version_object("HEAD~2...HEAD~1")
    assert e.value.error_code == ErrorCode.TUCE9


def test_build_version_object_with_version_object():
    initial_version = Version("HEAD^")
    built_version = build_version_object(initial_version)
    assert initial_version == built_version


def test_build_version_object_with_version_range_object():
    initial_version_range = VersionRange("HEAD~1", "HEAD")
    built_version_range = build_version_object(initial_version_range)
    assert initial_version_range == built_version_range


def test_build_version_object_with_version_list_object():
    initial_version_list = VersionList(["HEAD", "HEAD^^"])
    built_version_list = build_version_object(initial_version_list)
    assert initial_version_list == built_version_list


def test_build_version_object_with_version_string():
    initial_version = "HEAD^"
    built_version = build_version_object(initial_version)
    assert Version(initial_version) == built_version


def test_build_version_object_with_version_range_string():
    initial_version_range = "HEAD~1..HEAD"
    built_version_range = build_version_object(initial_version_range)
    assert VersionRange("HEAD~1", "HEAD") == built_version_range


def test_build_version_object_with_version_list_string():
    initial_version_list = "HEAD,HEAD^^"
    built_version_list = build_version_object(initial_version_list)
    assert VersionList(["HEAD", "HEAD^^"]) == built_version_list


def test_valid_versions_all_correct():
    version = "HEAD~1"
    assert Version(version)
    version_range = "HEAD^^"
    assert Version(version_range)
    version_list = "ADBDWEFW3434343434AFEF3F33"
    assert Version(version_list)


def test_invalid_version_raises_exception():
    version = "HEAD~1~HEAD"
    with pytest.raises(TableURIConfigurationError) as e:
        Version(version)
    assert e.value.error_code == ErrorCode.TUCE9
    version = "HEAD~1@HEAD"
    with pytest.raises(TableURIConfigurationError) as e:
        Version(version)
    assert e.value.error_code == ErrorCode.TUCE9
    version = "HEAD~^"
    with pytest.raises(TableURIConfigurationError) as e:
        Version(version)
    assert e.value.error_code == ErrorCode.TUCE9
    version = "1AFF3"
    with pytest.raises(TableURIConfigurationError) as e:
        Version(version)
    assert e.value.error_code == ErrorCode.TUCE9


def test_version_list_all_correct_list_string():
    version_list = ["HEAD~1", "12345678901234567890123456"]
    built_version_list = [Version(version) for version in version_list]
    version_list_object = VersionList(version_list)
    assert version_list_object.version_list == built_version_list
    assert version_list_object.to_string() == ",".join(version_list)
    assert build_version_object(version_list_object) == version_list_object
    assert build_version_object(version_list_object.to_string()) == version_list_object


def test_version_list_all_correct_list_object():
    version_list = ["HEAD~1", "12345678901234567890123456"]
    built_version_list = [Version(version) for version in version_list]
    version_list_object = VersionList(built_version_list)
    assert version_list_object.version_list == built_version_list
    assert version_list_object.to_string() == ",".join(version_list)
    assert build_version_object(version_list_object) == version_list_object
    assert build_version_object(version_list_object.to_string()) == version_list_object


def test_version_list_update():
    version_list = ["HEAD~1", "12345678901234567890123456"]
    built_version_list = [Version(version) for version in version_list]
    version_list_object = VersionList(version_list)
    assert version_list_object.version_list == built_version_list
    assert version_list_object.to_string() == ",".join(version_list)
    assert build_version_object(version_list_object) == version_list_object
    assert build_version_object(version_list_object.to_string()) == version_list_object
    second_version_list = ["HEAD^^", "HEAD~2"]
    second_built_version_list = [Version(version) for version in second_version_list]
    version_list_object.version_list = second_built_version_list
    assert version_list_object.version_list == second_built_version_list
    assert version_list_object.to_string() == ",".join(second_version_list)
    assert build_version_object(version_list_object) == version_list_object
    assert build_version_object(version_list_object.to_string()) == version_list_object


def test_wrong_type_version_list_raises_exception():
    version_int = 42
    with pytest.raises(TableURIConfigurationError) as e:
        VersionList(version_int)
    assert e.value.error_code == ErrorCode.TUCE6


def test_wrong_length_version_list_raises_exception():
    version_list = ["HEAD"]
    with pytest.raises(TableURIConfigurationError) as e:
        VersionList(version_list)
    assert e.value.error_code == ErrorCode.TUCE8


def test_uri_all_correct_version_string():
    collection = "collection"
    table = "table"
    version = "HEAD~1"
    uri = TableURI(collection, table, version)
    assert uri.collection == collection
    assert uri.table == table
    assert uri.version == Version(version)
    assert uri.to_string() == f"{collection}/{table}@{version}"
    assert build_table_uri_object(uri) == uri
    assert build_table_uri_object(uri.to_string()) == uri


def test_uri_all_correct_version():
    collection = "collection"
    table = "table"
    version = Version("HEAD~1")
    uri = TableURI(collection, table, version)
    assert uri.collection == collection
    assert uri.table == table
    assert uri.version == version
    assert uri.to_string() == f"{collection}/{table}@{version}"
    assert build_table_uri_object(uri) == uri
    assert build_table_uri_object(uri.to_string()) == uri


def test_uri_all_correct_version_list():
    collection = "collection"
    table = "table"
    version = VersionList(["HEAD~1", "HEAD~2"])
    uri = TableURI(collection, table, version)
    assert uri.collection == collection
    assert uri.table == table
    assert uri.version == version
    assert uri.to_string() == f"{collection}/{table}@{version}"
    assert build_table_uri_object(uri) == uri
    assert build_table_uri_object(uri.to_string()) == uri


def test_uri_all_correct_version_range():
    collection = "collection"
    table = "table"
    version = VersionRange("HEAD~1", "HEAD")
    uri = TableURI(collection, table, version)
    assert uri.collection == collection
    assert uri.table == table
    assert uri.version == version
    assert uri.to_string() == f"{collection}/{table}@{version}"
    assert build_table_uri_object(uri) == uri
    assert build_table_uri_object(uri.to_string()) == uri


def test_uri_collection_is_none():
    collection = None
    table = "table"
    version = Version("HEAD~1")
    uri = TableURI(collection, table, version)
    assert uri.collection == ""
    assert uri.table == table
    assert uri.version == version
    assert uri.to_string() == f"{table}@{version}"
    assert build_table_uri_object(uri) == uri
    assert build_table_uri_object(uri.to_string()) == uri


def test_uri_table_is_none_raises_error():
    collection = "collection"
    table = None
    version = Version("HEAD~1")
    with pytest.raises(TableURIConfigurationError) as e:
        TableURI(collection, table, version)
    assert e.value.error_code == ErrorCode.TUCE15


def test_uri_version_is_none():
    collection = "collection"
    table = "table"
    version = None
    uri = TableURI(collection, table, version)
    assert uri.collection == collection
    assert uri.table == table
    assert uri.version is None
    assert uri.to_string() == f"{collection}/{table}"
    assert build_table_uri_object(uri) == uri
    assert build_table_uri_object(uri.to_string()) == uri


def test_uri_update():
    collection = "collection"
    table = "table"
    version = Version("HEAD~1")
    uri = TableURI(collection, table, version)
    assert uri.collection == collection
    assert uri.table == table
    assert uri.version == version
    assert uri.to_string() == f"{collection}/{table}@{version}"
    assert build_table_uri_object(uri) == uri
    assert build_table_uri_object(uri.to_string()) == uri
    new_collection = "new_collection"
    new_table = "new_table"
    new_version = Version("HEAD~2")
    uri.collection = new_collection
    uri.table = new_table
    uri.version = new_version
    assert uri.collection == new_collection
    assert uri.table == new_table
    assert uri.version == new_version
    assert uri.to_string() == f"{new_collection}/{new_table}@{new_version}"
    assert build_table_uri_object(uri) == uri
    assert build_table_uri_object(uri.to_string()) == uri


def test_uri_wrong_type_collection_raises_type_error():
    with pytest.raises(TableURIConfigurationError) as e:
        TableURI(42, "table", Version("HEAD~1"))
    assert e.value.error_code == ErrorCode.TUCE10


def test_uri_wrong_type_table_raises_type_error():
    with pytest.raises(TableURIConfigurationError) as e:
        TableURI("collection", 42, Version("HEAD~1"))
    assert e.value.error_code == ErrorCode.TUCE12


def test_uri_wrong_type_version_raises_type_error():
    with pytest.raises(TableURIConfigurationError) as e:
        TableURI("collection", "table", 42)
    assert e.value.error_code == ErrorCode.TUCE4


def test_build_table_uri_object_wrong_type_raises_exception():
    with pytest.raises(TableURIConfigurationError) as e:
        build_table_uri_object(42)
    assert e.value.error_code == ErrorCode.TUCE14


def test_build_table_uri_object_wrong_regex_raises_exception():
    with pytest.raises(TableURIConfigurationError) as e:
        build_table_uri_object("dat@store/table@HEAD^")
    assert e.value.error_code == ErrorCode.TUCE13


def test_build_table_uri_object_with_uri_object():
    collection = "collection"
    table = "table"
    version = Version("HEAD~1")
    uri = TableURI(collection, table, version)
    assert build_table_uri_object(uri) == uri


def test_build_table_uri_object_with_uri_string():
    collection = "collection"
    table = "table"
    version = Version("HEAD~1")
    uri = f"{collection}/{table}@{version}"
    built_uri = TableURI(collection, table, version)
    assert build_table_uri_object(uri) == built_uri


def test_uri_list_all_valid():
    valid_uris = [
        "table",
        "collection/table",
        "table@HEAD",
        "collection/table@HEAD",
        "collection/table@HEAD^",
        "collection/table@HEAD~1",
        "collection/table@HEAD^^^^,HEAD^,HEAD",
        "collection/table@HEAD^^..HEAD",
    ]
    for uri in valid_uris:
        assert build_table_uri_object(uri).to_string() == uri
    TableURI(table="table")
    TableURI(collection="collection", table="table")
    TableURI(table="table", version="HEAD")
    TableURI(collection="collection", table="table", version="HEAD")
    TableURI(collection="collection", table="table", version="HEAD^")
    TableURI(collection="collection", table="table", version="HEAD~1")
    TableURI(
        collection="collection",
        table="table",
        version="HEAD^^^^,HEAD^,HEAD",
    )
    TableURI(collection="collection", table="table", version="HEAD^^..HEAD")


def test_uri_list_all_invalid():
    invalid_uris = [
        "td://collection",
        "/collection",
        "collection/",
        "/collection//",
        "collection//collection",
        "collection//",
        "collection/table/",
        "collection@head",
        "collection/table@HEAD-1",
        "collection/table@01234567890123456789012",
    ]
    for uri in invalid_uris:
        with pytest.raises(TableURIConfigurationError):
            build_table_uri_object(uri)


def test_version_not_equal_to_string():
    version = Version("HEAD~1")
    assert version != version.to_string()


def test_version_list_not_equal_to_string():
    version = VersionList(["HEAD~1", "HEAD~2"])
    assert version != version.to_string()


def test_version_range_not_equal_to_string():
    version = VersionRange("HEAD~1", "HEAD~2")
    assert version != version.to_string()


def test_uri_not_equal_to_string():
    uri = TableURI("collection", "table", Version("HEAD~1"))
    assert uri != uri.to_string()


def test_version_list_with_version_range_raises_exception():
    with pytest.raises(TableURIConfigurationError) as e:
        VersionList(["HEAD~1", "HEAD~2..HEAD"])
    assert e.value.error_code == ErrorCode.TUCE7
    with pytest.raises(TableURIConfigurationError) as e:
        VersionList([Version("HEAD~1"), VersionRange("HEAD~2", "HEAD")])
    assert e.value.error_code == ErrorCode.TUCE7
